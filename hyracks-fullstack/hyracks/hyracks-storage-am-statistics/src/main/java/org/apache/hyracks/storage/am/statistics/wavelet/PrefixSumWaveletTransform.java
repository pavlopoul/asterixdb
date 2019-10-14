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

import java.util.Stack;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.util.objectpool.IObjectFactory;
import org.apache.hyracks.util.objectpool.MapObjectPool;

public class PrefixSumWaveletTransform extends AbstractIntegerSynopsisBuilder<WaveletSynopsis> {
    private final Stack<WaveletCoefficient> avgStack;
    private MapObjectPool<WaveletCoefficient, Integer> avgStackObjectPool;
    protected long transformPosition;
    protected double transformFrequency;
    protected double prefixSumFrequency;

    public PrefixSumWaveletTransform(WaveletSynopsis synopsis, String dataverse, String dataset, String index,
            String field, boolean isAntimatter, IFieldExtractor fieldExtractor,
            ComponentStatistics componentStatistics) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        avgStack = new Stack<>();
        avgStackObjectPool = new MapObjectPool<>();
        IObjectFactory<WaveletCoefficient, Integer> waveletFactory = level -> new WaveletCoefficient(0.0, level, -1);
        for (int i = -1; i <= synopsis.getMaxLevel(); i++) {
            avgStackObjectPool.register(i, waveletFactory);
        }
        //add first dummy average
        WaveletCoefficient dummyCoeff = avgStackObjectPool.allocate(-1);
        dummyCoeff.setIndex(-1);
        avgStack.push(dummyCoeff);
        transformPosition = synopsis.getDomainStart();
        transformFrequency = 0.0;
        prefixSumFrequency = 0.0;
    }

    // Returns the parent wavelet coefficient for a given coefficient in the transform tree
    // Callee is responsible to deallocate the parent coefficient
    private WaveletCoefficient moveLevelUp(WaveletCoefficient childCoeff) {
        WaveletCoefficient parentCoeff = avgStackObjectPool.allocate(childCoeff.getLevel() + 1);
        parentCoeff.setValue(childCoeff.getValue());
        parentCoeff.setIndex(childCoeff.getParentCoeffIndex(synopsis.getDomainStart(), synopsis.getMaxLevel()));
        return parentCoeff;
    }

    // Calculates the position of the next tuple (on level 0) after given wavelet coefficient
    private long getTransformPosition(WaveletCoefficient coeff) {
        if (coeff.getLevel() < 0) {
            return synopsis.getDomainStart();
        } else if (coeff.getLevel() == 0) {
            return coeff.getIdx() + 1;
        } else {
            return ((((coeff.getIdx() + 1) << (coeff.getLevel() - 1)) - (1l << (synopsis.getMaxLevel() - 1))) << 1)
                    + synopsis.getDomainStart();
        }
    }

    // Combines two coeffs on the same level by averaging them and producing next level coefficient
    private void average(WaveletCoefficient leftCoeff, WaveletCoefficient rightCoeff, long domainMin, int maxLevel,
            WaveletCoefficient avgCoeff) {
        //        assert (leftCoeff.getLevel() == rightCoeff.getLevel());
        long coeffIdx = leftCoeff.getParentCoeffIndex(domainMin, maxLevel);
        // put detail wavelet coefficient to the coefficient queue
        synopsis.addElement(coeffIdx, (leftCoeff.getValue() - rightCoeff.getValue()) / 2.0, leftCoeff.getLevel() + 1,
                synopsis.isNormalized());
        avgCoeff.setIndex(coeffIdx);
        avgCoeff.setValue((leftCoeff.getValue() + rightCoeff.getValue()) / 2.0);
    }

    // Pushes given coefficient on the stack, possibly triggering domino effect
    private WaveletCoefficient pushToStack(WaveletCoefficient newCoeff) {
        // if the coefficient on the top of the stack has the same level as new coefficient, they should be combined
        while (!avgStack.isEmpty() && avgStack.peek().getLevel() == newCoeff.getLevel()) {
            WaveletCoefficient topCoeff = avgStack.pop();
            // Guard against dummy coefficients
            if (!topCoeff.isDummy()) {
                //allocate next level coefficient from objectPool
                WaveletCoefficient avgCoeff = avgStackObjectPool.allocate(topCoeff.getLevel() + 1);
                // combine newCoeff and topCoeff by averaging them. Result coeff's level is greater than parent's level by 1
                average(topCoeff, newCoeff, synopsis.getDomainStart(), synopsis.getMaxLevel(), avgCoeff);
                avgStackObjectPool.deallocate(topCoeff.getLevel(), topCoeff);
                avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
                newCoeff = avgCoeff;
            }
        }
        // Guard against dummy coefficients
        if (!newCoeff.isDummy()) {
            avgStack.push(newCoeff);
        }
        return newCoeff;
    }

    protected void transformTuple(long tuplePosition, double prefixSumValue, double tupleValue) {
        // 2nd part: Downward transform
        // calculate the tuple position, where the transform currently stopped
        long transformPosition = getTransformPosition(avgStack.peek());
        // put all the coefficients, corresponding to dyadic ranges between current tuple position & transformPosition on the stack
        computeDyadicSubranges(tuplePosition, transformPosition, prefixSumValue);
        // put the last coefficient, corresponding to current tuple position on to the stack
        WaveletCoefficient newCoeff = avgStackObjectPool.allocate(0);
        newCoeff.setValue(prefixSumValue + tupleValue);
        newCoeff.setIndex(tuplePosition);
        pushToStack(newCoeff);
        //avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
    }

    // Method calculates decreasing level dyadic intervals between tuplePosition&currTransformPosition and saves corresponding coefficients in the avgStack
    // tuplePosition: position of the inserted tuple
    // currTransformPosition: position at which transform algorithm stopped
    // prefixSum: value of the prefix sum for all tuples in [currTransformPosition, tuplePosition)
    private void computeDyadicSubranges(long tuplePosition, long currTransformPosition, double prefixSum) {
        while (tuplePosition != currTransformPosition) {
            WaveletCoefficient coeff;
            if (avgStack.size() > 0) {
                coeff = avgStackObjectPool.allocate(avgStack.peek().getLevel());
                coeff.setValue(prefixSum);
                // starting with the sibling of the top coefficient on the stack
                coeff.setIndex(avgStack.peek().getIdx() + 1L);
            }
            // special case when there is no coeffs on the stack.
            else {
                coeff = avgStackObjectPool.allocate(synopsis.getMaxLevel());
                coeff.setValue(prefixSum);
                // Starting descent from top coefficient, i.e. the one with index == 1, level == maxLevel
                coeff.setIndex(1l);
            }
            // decrease the coefficient level until it stops covering tuplePosition
            while (coeff.covers(tuplePosition, synopsis.getMaxLevel(), synopsis.getDomainStart())) {
                avgStackObjectPool.deallocate(coeff.getLevel(), coeff);
                WaveletCoefficient newCoeff = avgStackObjectPool.allocate(coeff.getLevel() - 1);
                newCoeff.setValue(prefixSum);
                if (newCoeff.getLevel() == 0) {
                    newCoeff.setIndex(
                            ((coeff.getIdx() - (1l << (synopsis.getMaxLevel() - 1))) << 1) + synopsis.getDomainStart());
                } else {
                    newCoeff.setIndex(coeff.getIdx() << 1);
                }
                coeff = newCoeff;
            }
            // we don't add newCoeff to the wavelet coefficient collection, since it's value is 0. Keep it only in average stack
            pushToStack(coeff);
            currTransformPosition = getTransformPosition(coeff);
        }
    }

    @Override
    public void addValue(long tuplePosition) {
        // check whether tuple with this position was already seen
        if (transformPosition != tuplePosition) {
            transformTuple(transformPosition, prefixSumFrequency, transformFrequency);
            transformPosition = tuplePosition;
            prefixSumFrequency += transformFrequency;
            transformFrequency = 0;
        }
        transformFrequency += 1.0;
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        //complete transform
        transformTuple(transformPosition, prefixSumFrequency, transformFrequency);
        if (transformPosition != synopsis.getDomainEnd()) {
            transformTuple(synopsis.getDomainEnd(), prefixSumFrequency + transformFrequency, 0.0);
        }
        WaveletCoefficient topCoeff = avgStack.pop();
        // transform is complete the top coefficient on the stack is global average, i.e. coefficient with index==0
        synopsis.addElement(0l, topCoeff.getValue(), synopsis.getMaxLevel(), synopsis.isNormalized());
        avgStackObjectPool.deallocate(topCoeff.getLevel(), topCoeff);

        synopsis.createBinaryPreorder();
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
