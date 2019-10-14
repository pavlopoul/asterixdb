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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;

public class EquiWidthHistogramSynopsis extends HistogramSynopsis<HistogramBucket> {
    private final long bucketWidth;

    public EquiWidthHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, int bucketsNum,
            Collection<HistogramBucket> synopsisElements) {
        super(domainStart, domainEnd, maxLevel, bucketsNum, synopsisElements);
        //TODO:avoid numeric overflows
        bucketWidth = (domainEnd - domainStart + 1) / bucketsNum;
        long border = domainStart + bucketWidth;
        for (int i = 0; i < bucketsNum; i++) {
            getBuckets().add(new HistogramBucket(border, 0.0));
            border += bucketWidth;
        }
        getBuckets().get(bucketsNum - 1).setRightBorder(domainEnd);
    }

    @Override
    public void appendToBucket(int bucketId, int bucketNum, long tuplePos, double frequency) {
        getBuckets().get(bucketId).appendToValue(frequency);
    }

    @Override
    public boolean advanceBucket(int activeBucket, int activeBucketElementsNum, long currTuplePosition,
            long lastAddedTuplePosition) {
        return currTuplePosition != lastAddedTuplePosition
                && currTuplePosition > getBuckets().get(activeBucket).getKey();
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.EquiWidthHistogram;
    }

    @Override
    public void merge(ISynopsis<HistogramBucket> mergeSynopsis) throws HyracksDataException {
        EquiWidthHistogramSynopsis histogramToMerge = (EquiWidthHistogramSynopsis) mergeSynopsis;
        if (getSize() != histogramToMerge.getSize())
            throw new HyracksDataException("Cannot merge equi-width histograms with different number of buckets");
        for (int i = 0; i < getSize(); i++) {
            getBuckets().get(i).appendToValue(histogramToMerge.getBuckets().get(i).getValue());
        }
    }
}
