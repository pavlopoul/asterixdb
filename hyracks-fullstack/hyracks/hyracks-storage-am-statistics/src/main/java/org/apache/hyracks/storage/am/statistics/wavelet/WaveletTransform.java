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
package org.apache.hyracks.storage.am.statistics.wavelet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/// Algorithm follows "Straddling coefficients" algorithm outline, presented in "Surfing Wavelets on Streams: One-Pass
/// Summaries for Approximate Aggregate Queries" by Gilbert et al.
public class WaveletTransform extends AbstractIntegerSynopsisBuilder<WaveletSynopsis> {
    private long transformPosition;
    private double transformFrequency;
    private WaveletCoefficient[] straddlingCoeffs;

    public WaveletTransform(WaveletSynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, IFieldExtractor fieldExtractor, ComponentStatistics componentStatistics) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        // keep straddling coefficients for each level of error tree + additional coefficient for main average
        straddlingCoeffs = new WaveletCoefficient[synopsis.getMaxLevel() + 1];
        for (int i = 0; i < synopsis.getMaxLevel(); i++) {
            straddlingCoeffs[i] = new WaveletCoefficient(0.0, -1, -1L);
        }
        straddlingCoeffs[synopsis.getMaxLevel()] = new WaveletCoefficient(0.0, synopsis.getMaxLevel() + 1, 0L);
    }

    @Override
    public void addValue(long tuplePosition) {
        // check whether tuple with this position was already seen
        if (transformPosition != tuplePosition && !isEmpty) {
            transformTuple(transformPosition, transformFrequency);
            transformFrequency = 0;
        }
        transformFrequency += 1.0;
        transformPosition = tuplePosition;
        isEmpty = false;
    }

    private void transformTuple(long tuplePosition, double frequency) {
        WaveletCoefficient coeff = new WaveletCoefficient(frequency, 0, tuplePosition);
        int sign = (((tuplePosition - synopsis.getDomainStart()) & 0x1) == 1) ? -1 : 1;
        for (int i = 0; i < synopsis.getMaxLevel(); i++) {
            coeff.reset(Math.abs(coeff.getValue()) * sign / 2, coeff.getLevel() + 1,
                    coeff.getParentCoeffIndex(synopsis.getDomainStart(), synopsis.getMaxLevel()));
            sign = ((coeff.getIdx() & 0x1) == 1) ? -1 : 1;
            if (straddlingCoeffs[i].covers(tuplePosition, synopsis.getMaxLevel(), synopsis.getDomainStart())) {
                straddlingCoeffs[i].reset(straddlingCoeffs[i].getValue() + coeff.getValue(), coeff.getLevel(),
                        coeff.getIdx());
            } else {
                // tuple's position is not covered by the straddling coefficient C anymore, i.e. transform for C is
                // finished. Add C to priority queue if the coefficient is not dummy and renew straddling coefficient.
                if (!straddlingCoeffs[i].isDummy()) {
                    synopsis.addElement(straddlingCoeffs[i], synopsis.isNormalized());
                }
                straddlingCoeffs[i].reset(coeff);
            }
        }
        //update main average
        straddlingCoeffs[synopsis.getMaxLevel()]
                .setValue(straddlingCoeffs[synopsis.getMaxLevel()].getValue() + Math.abs(coeff.getValue()));
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {//complete transform
        transformTuple(transformPosition, transformFrequency);
        // finish all straddling coefficients
        for (int i = 0; i <= synopsis.getMaxLevel(); i++) {
            synopsis.addElement(straddlingCoeffs[i], synopsis.isNormalized());
        }
        synopsis.orderByKeys();
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
