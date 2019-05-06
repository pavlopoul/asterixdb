/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.statistics.historgram;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public abstract class HistogramSynopsis<T extends HistogramBucket> extends AbstractSynopsis<T> {
    public HistogramSynopsis(long domainStart, long domainEnd, int maxLevel, int bucketsNum,
            Collection<T> synopsisElements, Map<Long, Integer> uniqueMap) {
        super(domainStart, domainEnd, maxLevel, bucketsNum, synopsisElements, uniqueMap);
    }

    //implicit cast to operate with buckets as a list
    protected List<T> getBuckets() {
        return (List<T>) synopsisElements;
    }

    protected long getBucketSpan(int bucketId) {
        long start = getBucketStartPosition(bucketId);
        return getBuckets().get(bucketId).getKey() - start + 1;
    }

    protected long getBucketStartPosition(int idx) {
        return idx == 0 ? domainStart : getBuckets().get(idx - 1).getKey() + 1;
    }

    protected int getPointBucket(long position) {
        int idx = Collections.binarySearch(getBuckets(), new HistogramBucket(position, 0.0, 0, 0),
                Comparator.comparingLong(HistogramBucket::getKey));
        if (idx < 0) {
            idx = -idx - 1;
        }
        return idx;
    }

    @Override
    public double pointQuery(long position) {
        int idx = getPointBucket(position);
        return approximateValueWithinBucket(idx, position, position + 1);
    }

    @Override
    public double rangeQuery(long startPosition, long endPosition) {
        int startBucket = getPointBucket(startPosition);
        int endBucket = getPointBucket(endPosition);
        if (startBucket == getBuckets().size()) {
            return 0.0;
        }
        if (endBucket == getBuckets().size()) {
            endBucket = endBucket - 1;
        }
        if (this.getType() == SynopsisType.ContinuousHistogram) {
            endPosition = endPosition > getBuckets().get(endBucket).getKey() ? getBuckets().get(endBucket).getKey()
                    : endPosition;
        }
        long endBucketLeftBorder = getBucketStartPosition(endBucket);
        double value = 0.0;
        if (startBucket == endBucket) {
            value = approximateValueWithinBucket(startBucket, startPosition, endPosition);
        } else {
            //account for part of the initial bucket between startPosition and it's right border
            value += approximateValueWithinBucket(startBucket, startPosition, getBuckets().get(startBucket).getKey());
            //...and for the part between left border of the last bucket and endPosition
            value += approximateValueWithinBucket(endBucket, endBucketLeftBorder, endPosition);
            //sum up all the buckets in between
            for (int i = startBucket + 1; i < endBucket; i++) {
                value += getBuckets().get(i).getValue();
            }
        }
        return value;
    }

    @Override
    public double joinQuery(ISynopsis synopsis, boolean primIndex) {
        HistogramSynopsis<T> histogram = (HistogramSynopsis<T>) synopsis;
        double leftEstimate = 0.0;
        double rightEstimate = 0.0;
        double estimate = 0.0;
        for (int i = 0; i < getBuckets().size(); i++) {
            if (getBuckets().get(i).getValue() != 0.0) {
                leftEstimate += getBuckets().get(i).getValue();

            }
        }

        for (int i = 0; i < histogram.getBuckets().size(); i++) {
            if (histogram.getBuckets().get(i).getValue() != 0.0) {
                rightEstimate += histogram.getBuckets().get(i).getValue();

            }
        }
        estimate = leftEstimate * rightEstimate;
        return estimate;
    }

    @Override
    public long uniqueQuery(boolean primIndex) {
        long distinctValues = 0;
        if (this.getType() == SynopsisType.ContinuousHistogram) {
            primIndex = false;
        }
        if (primIndex) {
            for (int i = 0; i < getBuckets().size(); i++) {
                if (getBuckets().get(i).getValue() != 0) {
                    distinctValues += getBuckets().get(i).getValue();
                }
            }
        } else {
            for (int i = 0; i < getBuckets().size(); i++) {
                if (getBuckets().get(i).getUniqueValue() != 0) {
                    distinctValues += getBuckets().get(i).getUniqueValue();
                }
            }
        }

        return distinctValues;
    }

    public double approximateValueWithinBucket(int bucketIdx, long startPosition, long endPosition) {
        return getBuckets().get(bucketIdx).getValue() * (endPosition - startPosition + 1) / getBucketSpan(bucketIdx);
    }

    public abstract void appendToBucket(int bucketId, int bucketNum, long tuplePos, double frequency);

    public abstract boolean advanceBucket(int activeBucket, int activeBucketElementsNum, long currTuplePosition,
            long lastAddedTuplePosition);

    public void finishBucket(int activeBucket) {
    }
}
