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

import java.util.List;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;

public abstract class EquiHeightHistogramSynopsis<T extends HistogramBucket> extends HistogramSynopsis<T> {

    private static final long serialVersionUID = 1L;
    private final long elementsPerBucket;

    public EquiHeightHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum, int bucketsNum,
            List<T> buckets) {
        super(domainStart, domainEnd, maxLevel, bucketsNum, buckets);
        elementsPerBucket = Math.max((long) Math.ceil((double) elementsNum / bucketsNum), 1);
    }

    public long getElementsPerBucket() {
        return elementsPerBucket;
    }

    @Override
    public void merge(ISynopsis<T> mergeSynopsis) {
        throw new UnsupportedOperationException();
    }

    public void setBucketBorder(int bucket, long border) {
        getBuckets().get(bucket).setRightBorder(border);
    }

    public boolean advanceBucket(int activeBucket, int activeBucketElementsNum, long currTuplePosition,
            long lastAddedTuplePosition) {
        if (activeBucket <= size - 1 && currTuplePosition != lastAddedTuplePosition
                && activeBucketElementsNum >= elementsPerBucket) {
            setBucketBorder(activeBucket, currTuplePosition - 1);
            return true;
        }
        return false;
    }

    @Override
    public void finishBucket(int activeBucket) {
        setBucketBorder(activeBucket, getDomainEnd());
    }
}
