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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class HistogramBuilder extends AbstractIntegerSynopsisBuilder<HistogramSynopsis<? extends HistogramBucket>> {

    private int activeBucket;
    private int activeBucketElementsNum;
    private long lastAddedTuplePosition;

    public HistogramBuilder(HistogramSynopsis<? extends HistogramBucket> histogram, String dataverse, String dataset,
            String index, String field, boolean isAntimatter, IFieldExtractor fieldExtractor,
            ComponentStatistics componentStatistics) {
        super(histogram, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        activeBucket = 0;
        activeBucketElementsNum = 0;
        lastAddedTuplePosition = synopsis.getDomainStart();
    }

    @Override
    public void addValue(long currTuplePosition) {
        while (synopsis.advanceBucket(activeBucket, activeBucketElementsNum, currTuplePosition,
                lastAddedTuplePosition)) {
            activeBucket++;
            activeBucketElementsNum = 0;
        }
        synopsis.appendToBucket(activeBucket, synopsis.getBuckets().size(), currTuplePosition, 1.0);
        activeBucketElementsNum++;
        lastAddedTuplePosition = currTuplePosition;
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        if (activeBucketElementsNum == 0) {
            synopsis.appendToBucket(0, 0, synopsis.getDomainEnd(), 0.0);
        }
        synopsis.finishBucket(activeBucket);
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean hasFailed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Throwable getFailure() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}
