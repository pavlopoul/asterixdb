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
package org.apache.hyracks.storage.am.statistics.sketch.groupcount;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class GroupCountSketchBuilder extends AbstractIntegerSynopsisBuilder<WaveletSynopsis> {
    private GroupCountHierarchicalSketch<Long> hierarchicalSketch;

    public GroupCountSketchBuilder(WaveletSynopsis synopsis, String dataverse, String dataset, String index,
            String field, boolean isAntimatter, IFieldExtractor fieldExtractor, ComponentStatistics componentStatistics,
            int fanout, double failureProbability, double accuracy, double energyAccuracy, long inputSize, long seed) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        hierarchicalSketch = new GroupCountHierarchicalSketch(synopsis.getDomainStart(), synopsis.getMaxLevel(),
                synopsis.isNormalized(), synopsis.getSize(), fanout, failureProbability, accuracy, energyAccuracy,
                inputSize, seed);
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        for (WaveletCoefficient w : hierarchicalSketch.finish()) {
            // coeff value was already normalized when it was added to the sketch
            boolean isNormalized = false;
            synopsis.addElement(w, isNormalized);
        }
        synopsis.orderByKeys();
    }

    @Override
    public void addValue(long value) {
        hierarchicalSketch.insert(value);
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
