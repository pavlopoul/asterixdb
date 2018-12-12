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

import java.util.Collection;

import org.apache.commons.collections4.iterators.PeekingIterator;

public class PrefixSumWaveletSynopsis extends WaveletSynopsis {

    private static final long serialVersionUID = 1L;

    public PrefixSumWaveletSynopsis(long domainStart, long domainEnd, int maxLevel, int size,
            Collection<WaveletCoefficient> coefficients, boolean normalize, boolean linearApproximation) {
        super(domainStart, domainEnd, maxLevel, size, coefficients, normalize, linearApproximation);
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.PrefixSumWavelet;
    }

    @Override
    public double pointQuery(long position) {
        if (position == getDomainStart()) {
            return getApproximatedPrefixSum(position);
        } else {
            return getApproximatedPrefixSum(position) - getApproximatedPrefixSum(position - 1);
        }
    }

    private DyadicTupleRange getPrefixSum(PeekingIterator<WaveletCoefficient> it, double average, long startPosition,
            long startCoeffIdx, int level) {
        double coeffVal = 0.0;
        long coeffIdx = 0;
        DyadicTupleRange result = convertCoeffToSupportInterval(startPosition, 0);
        // start reverse decomposition starting from the top coefficient (i.e. with level=maxLevel)
        for (int i = level; i >= 0; i--) {
            if (i == 0) {
                //on the last level use position to calculate sign of the coefficient
                coeffIdx = startPosition - domainStart;
            } else {
                coeffIdx = startCoeffIdx >>> (i - 1);
            }
            if ((coeffIdx & 0x1) == 1) {
                coeffVal *= -1;
            }
            if (coeffVal != 0.0) {
                if (i == 0) {
                    result = new DyadicTupleRange(startPosition, startPosition, 0.0);
                } else {
                    result = convertCoeffToSupportInterval(coeffIdx, i);
                }
            }
            average += coeffVal;
            coeffVal = findCoeffValue(it, coeffIdx, i);
            if (normalize)
                coeffVal *= WaveletCoefficient.getNormalizationCoefficient(maxLevel, i);
        }
        result.setValue(average);
        return result;
    }

    private double getApproximatedPrefixSum(long position) {
        //find a prefix sum and it's support interval for given position
        DyadicTupleRange positionDyadicRange = getPrefixSum(position);
        if (linearApproximation) {
            //if a range is a single point there is not need for approximation
            if (positionDyadicRange.getStart() == positionDyadicRange.getEnd())
                return positionDyadicRange.getValue();
            //indicates whether the position is located in the first half of the appropriate dyadic range or not
            boolean firstHalf = position < (positionDyadicRange.getStart() + positionDyadicRange.getEnd()) / 2.0;
            //        double approximationXStart = (positionDyadicRange.getStart() + positionDyadicRange.getEnd()) / 2.0;
            double stairLength = (positionDyadicRange.getEnd() - positionDyadicRange.getStart()) / 2.0 + 1;
            double x = position;
            DyadicTupleRange prevDyadicRange;
            DyadicTupleRange nextDyadicRange;
            if (firstHalf) {
                nextDyadicRange = positionDyadicRange;
                if (positionDyadicRange.getStart() == domainStart) {
                    prevDyadicRange = new DyadicTupleRange(domainStart, domainStart, 0.0);
                    x += 1;
                } else {
                    prevDyadicRange = getPrefixSum(positionDyadicRange.getStart() - 1);
                    stairLength += (prevDyadicRange.getEnd() - prevDyadicRange.getStart()) / 2.0;
                }
            } else {
                prevDyadicRange = positionDyadicRange;
                if (positionDyadicRange.getEnd() == domainEnd) {
                    DyadicTupleRange prev = getPrefixSum(positionDyadicRange.getStart() - 1);
                    nextDyadicRange = new DyadicTupleRange(domainEnd, domainEnd,
                            positionDyadicRange.getValue() + (positionDyadicRange.getValue() - prev.getValue()) / 2);
                    stairLength -= 0.5;
                } else {
                    nextDyadicRange = getPrefixSum(positionDyadicRange.getEnd() + 1);
                    stairLength += (nextDyadicRange.getEnd() - nextDyadicRange.getStart()) / 2.0;
                }
            }
            x -= (prevDyadicRange.getStart() + prevDyadicRange.getEnd()) / 2.0;
            double stairHeight = nextDyadicRange.getValue() - prevDyadicRange.getValue();
            return prevDyadicRange.getValue() + x * stairHeight / stairLength;
        } else {
            return positionDyadicRange.getValue();
        }
    }

    private DyadicTupleRange getPrefixSum(long position) {
        PeekingIterator<WaveletCoefficient> it = new PeekingIterator<>(synopsisElements.iterator());
        long startCoeffIdx = convertPositionToCoeffIndex(position);
        double mainAvg = findCoeffValue(it, 0l, maxLevel);
        return getPrefixSum(it, mainAvg, position, startCoeffIdx, maxLevel);
    }

    @Override
    public double rangeQuery(long startPosition, long endPosition) {
        double startSum = 0.0;
        if (startPosition > getDomainStart()) {
            startSum = getApproximatedPrefixSum(startPosition - 1);
        }
        return getApproximatedPrefixSum(endPosition) - startSum;
    }

    //TODO:replace with WaveletCoefficient method
    public DyadicTupleRange convertCoeffToSupportInterval(long index, int level) {
        long intervalStart = convertCoeffToPosition(index, level);
        return new DyadicTupleRange(intervalStart, intervalStart + (1L << level) - 1, 0.0);
    }

    //TODO:replace with WaveletCoefficient method
    public long convertCoeffToPosition(long index, int level) {
        // Position is calculated via formula coeff << (maxLevel - level) - domainLength + domainStart.
        // Domain length is expanded like domainEnd-domainStart, to avoid integer overflow
        //return (coeff << level) - domainEnd + domainStart + domainStart;

        if (level == 0)
            return index;
        else {
            //binary mask, used to zero out most significant bits
            long mask = domainEnd - domainStart;
            return ((index << level) & mask) + domainStart;
        }
    }
}