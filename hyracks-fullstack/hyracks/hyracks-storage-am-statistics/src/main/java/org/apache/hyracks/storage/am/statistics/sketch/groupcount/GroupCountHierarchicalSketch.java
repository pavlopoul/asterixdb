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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.hyracks.storage.am.statistics.sketch.ISketch;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class GroupCountHierarchicalSketch<T extends Number> implements ISketch<T, WaveletCoefficient> {
    private final static Logger LOGGER = Logger.getLogger(GroupCountSketch.class.getName());
    private final long domainStart;
    private final int maxLevel;
    private final boolean normalize;
    private int size;
    private final int fanout;
    private final int levelNum;
    private final double energyThreshold;

    private GroupCountSketch[] sketches;

    public GroupCountHierarchicalSketch(long domainStart, int maxLevel, boolean normalize, int synopsisSize, int fanout,
            double failureProbability, double accuracy, double energyAccuracy, long inputSize, long seed) {
        this.domainStart = domainStart;
        this.maxLevel = maxLevel;
        this.normalize = normalize;
        this.fanout = fanout;
        this.levelNum = maxLevel / fanout;

        // number of independent testsNum used to randomize sketch is determined as O(log(1/δ))
        int testsNum = (int) Math.round(Math.log(1.0 / failureProbability));
        // number of buckets is determined as O(1/ε)
        int bucketNum = (int) Math.round(1.0 / accuracy);
        // number of subbuckets is determined as O(1/(ε^2))
        int subbucketNum = bucketNum * bucketNum;
        // threshold 𝜙 is determined as stream size multiplied by (𝜂*ε)/B
        if (inputSize > 0) {
            energyThreshold = inputSize * (accuracy * energyAccuracy) / synopsisSize;
        } else {
            energyThreshold = Double.MAX_VALUE;
        }

        Random random = new Random(seed);
        // Use MurmurHash to generate pairwise independent hash functions. Refer to "Less hashing, same performance:
        // Building a better bloom filter" ESA'06 for explanations
        HashFunction murmurHash = Hashing.murmur3_128((int) seed);

        sketches = new GroupCountSketch[levelNum + 1];
        this.size = 0;
        for (int i = 0; i <= levelNum; i++) {
            sketches[i] = new GroupCountSketch(testsNum, bucketNum, subbucketNum, random, murmurHash);
            this.size += sketches[i].getSize();
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void insert(T position) {
        double value = 1.0;
        WaveletCoefficient coeff = new WaveletCoefficient(value, 0, position.longValue());
        //translate adding coeff on level 0 to updates in upper levels of error tree
        while (coeff.getIdx() != 0 || coeff.getLevel() == 0) {
            long parentIdx = coeff.getParentCoeffIndex(domainStart, maxLevel);
            double parentValue = value;
            int parentLevel;
            //main average coefficient
            if (coeff.getLevel() == maxLevel) {
                parentLevel = coeff.getLevel();
            } else {
                int sign = ((coeff.getIdx() & 0x1) == 1) ? -1 : 1;
                parentValue *= sign;
                parentLevel = coeff.getLevel() + 1;
            }
            parentValue /= (1L << parentLevel);
            if (normalize) {
                parentValue /= WaveletCoefficient.getNormalizationCoefficient(maxLevel, parentLevel * fanout);
            }
            coeff.reset(parentValue, parentLevel, parentIdx);
            updateSketchBinaryTree(parentIdx, parentValue);
        }

        //        item += 1 << (levelNum * fanout);
        //transform update into wavelet domain
        //        long div = 1;
        //        for (int i = 0; i < levelNum; i++) {
        //            //            Long coeffIdx = (long) ((1 << ((levelNum - i) * fanout)) + item);
        //            item >>= (fanout - 1);
        //            int sign = (item & 1) == 0 ? 1 : -1;
        //            item >>= 1;
        //            double normCoeff = WaveletCoefficient.getNormalizationCoefficient(levelNum * fanout,
        //                    (i + 1) * fanout);
        //            div = (1 << ((i + 1) * fanout));
        //
        //            gcSketch.update(item, diff * sign / (normCoeff * div));
        //        }
        //        gcSketch.update(0, diff / div);

        //for (j = 0, group = coeffIdx; j < levels; j += fanout, group >>= fanout) {
    }

    private void updateSketchBinaryTree(long coeffIdx, double coeffValue) {
        long groupIdx = coeffIdx;
        for (int i = 0; i <= levelNum; i++, groupIdx >>= fanout) {
            sketches[i].update(coeffIdx, groupIdx, coeffValue);
        }
    }

    @Override
    public List<WaveletCoefficient> finish() {
        List<WaveletCoefficient> levelCoeffs = new LinkedList<>();
        int level = 0;
        while (level <= maxLevel)
            level += fanout;
        level -= fanout;

        //recursively traverse sketches of various levels to get the highest-energy coefficients
        for (int i = 0; i < (1L << (maxLevel - level)); i++) {
            levelCoeffs.addAll(findHighestEnergyCoeffs(levelNum, i));
        }
        return levelCoeffs;
    }

    private List<WaveletCoefficient> findHighestEnergyCoeffs(int level, long groupIdx) {
        List<WaveletCoefficient> results = new LinkedList<>();
        double energyEstimate = sketches[level].estimateL2(groupIdx);
        if (energyEstimate >= energyThreshold) {
            if (level == 0) {
                double coeffValue = sketches[level].estimateValue(groupIdx, groupIdx);
                results.add(
                        new WaveletCoefficient(coeffValue, WaveletCoefficient.getLevel(groupIdx, maxLevel), groupIdx));
            } else {
                for (int i = 0; i < (1L << fanout); i++) {
                    long childGroupIdx = (groupIdx << fanout) + i;
                    results.addAll(findHighestEnergyCoeffs(level - 1, childGroupIdx));
                }
            }
        }
        return results;
    }
}
