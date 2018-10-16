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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public abstract class HistogramSynopsis<T extends HistogramBucket> extends AbstractSynopsis<T> {
    public HistogramSynopsis(long domainStart, long domainEnd, int maxLevel, int bucketsNum,
            Collection<T> synopsisElements) {
        super(domainStart, domainEnd, maxLevel, bucketsNum, synopsisElements);
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
        int idx = Collections.binarySearch(getBuckets(), new HistogramBucket(position, 0.0),
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
        if (endBucket == getBuckets().size()) {
            endBucket = endBucket - 1;
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
    public double joinQuery(ISynopsis synopsis) {
        HistogramSynopsis<T> histogram = (HistogramSynopsis<T>) synopsis;
        double leftEstimate = 0.0;
        double rightEstimate = 0.0;
        List<long[]> leftBuckets = new ArrayList<>();
        List<long[]> rightBuckets = new ArrayList<>();
        for (int i = 0; i < getBuckets().size(); i++) {
            if (getBuckets().get(i).getValue() != 0.0) {
                long startPosition = getBucketStartPosition(i);
                long array[] = { startPosition, getBuckets().get(i).getKey() };
                leftBuckets.add(array);

            }
        }

        for (int i = 0; i < histogram.getBuckets().size(); i++) {
            if (histogram.getBuckets().get(i).getValue() != 0.0) {
                long startPosition = histogram.getBucketStartPosition(i);
                long array[] = { startPosition, histogram.getBuckets().get(i).getKey() };
                rightBuckets.add(array);

            }
        }
        for (long[] arrayL : leftBuckets) {
            for (long[] arrayR : rightBuckets) {
                if (arrayL[0] <= arrayR[0] && arrayL[1] >= arrayR[1]) {
                    leftEstimate += rangeQuery(arrayR[0], arrayR[1]);
                    rightEstimate += histogram.rangeQuery(arrayR[0], arrayR[1]);
                } else if (arrayL[0] <= arrayR[0] && arrayL[1] <= arrayR[1] && arrayL[1] >= arrayR[0]) {
                    leftEstimate += rangeQuery(arrayR[0], arrayL[1]);
                    rightEstimate += histogram.rangeQuery(arrayR[0], arrayL[1]);
                } else if (arrayL[0] >= arrayR[0] && arrayL[1] >= arrayR[1] && arrayL[0] <= arrayR[1]) {
                    leftEstimate += rangeQuery(arrayL[0], arrayR[1]);
                    rightEstimate += histogram.rangeQuery(arrayL[0], arrayR[1]);
                } else if (arrayL[0] >= arrayR[0] && arrayL[1] <= arrayR[1]) {
                    leftEstimate += rangeQuery(arrayL[0], arrayL[1]);
                    rightEstimate += histogram.rangeQuery(arrayL[0], arrayL[1]);
                }
            }
        }

        return Math.max(rightEstimate, leftEstimate);
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
