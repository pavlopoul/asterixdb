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
package org.apache.hyracks.storage.am.statistics.sketch.quantile;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.am.statistics.historgram.EquiHeightHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class QuantileSketchBuilder
        extends AbstractIntegerSynopsisBuilder<EquiHeightHistogramSynopsis<HistogramBucket>> {

    private QuantileSketch<Long> sketch;

    public QuantileSketchBuilder(EquiHeightHistogramSynopsis synopsis, String dataverse, String dataset, String index,
            String field, boolean isAntimatter, IFieldExtractor fieldExtractor, ComponentStatistics componentStatistics,
            double accuracy) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        sketch = new QuantileSketch<>(synopsis.getSize(), synopsis.getDomainStart(), accuracy);
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        //extract quantiles from the sketch, i.e. create an equi-height histogram
        List<Long> ranks = sketch.finish();
        long height = Math.max(sketch.length() / (Math.max(ranks.size(), 1)), 1);
        // take into account that rank values could contain duplicates
        Long prev = null;
        long bucketHeight = 0;
        for (Long r : ranks) {
            if (prev != null && r != prev) {
                synopsis.getElements().add(new HistogramBucket(prev, bucketHeight));
                bucketHeight = 0;
            }
            bucketHeight += height;
            prev = r;
        }
        if (prev != null) {
            synopsis.getElements().add(new HistogramBucket(prev, bucketHeight));
        }
    }

    @Override
    public void addValue(long value) {
        sketch.insert(value);
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
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
