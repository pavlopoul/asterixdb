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

import java.util.Random;

import org.apache.hyracks.data.std.util.BufferSerDeUtil;

import com.google.common.hash.HashFunction;

public class GroupCountSketch {

    private final int testsNum;
    private final int bucketNum;
    private final int subbucketNum;
    private final double counters[][][];

    private final long[][] bucketHashes;
    private final long[][] subbucketHashes;
    private final long[][] itemHashes;

    public GroupCountSketch(int testsNum, int bucketNum, int subbucketNum, Random random, HashFunction hashFunction) {
        this.testsNum = testsNum;
        this.bucketNum = bucketNum;
        this.subbucketNum = subbucketNum;

        counters = new double[testsNum][bucketNum][subbucketNum];
        bucketHashes = new long[testsNum][2];
        subbucketHashes = new long[testsNum][2];
        itemHashes = new long[testsNum][4];
        initSeeds(bucketHashes, random, hashFunction);
        initSeeds(subbucketHashes, random, hashFunction);
        initSeeds(itemHashes, random, hashFunction);
    }

    public int getSize() {
        return testsNum * bucketNum * subbucketNum * Long.BYTES;
    }

    private void initSeeds(long[][] hashSeeds, Random random, HashFunction murmurHash) {
        for (int i = 0; i < hashSeeds.length; i++) {
            for (int j = 0; j < hashSeeds[0].length; j += 2) {
                long randLong = random.nextLong();
                if (randLong < 0 && randLong != Long.MIN_VALUE) {
                    randLong *= -1;
                } else if (randLong == Long.MIN_VALUE) {
                    randLong = Long.MAX_VALUE;
                }
                byte[] hash = murmurHash.hashLong(randLong).asBytes();
                //extract two 64-bit hashes
                hashSeeds[i][j] = BufferSerDeUtil.getLong(hash, 0);
                hashSeeds[i][j + 1] = BufferSerDeUtil.getLong(hash, 8);
            }
        }
    }

    // Updates values inside GC sketch.
    // Operates in *wavelet* domain, i.e., inputs correspond to appropriate wavelet coefficient's index & value
    public void update(long coeffIdx, long groupIdx, double diff) {
        long[] products = HashGenerator.productVector(coeffIdx);
        for (int i = 0; i < testsNum; i++) {
            int groupHash = HashGenerator.pairwiseIndependent(bucketHashes[i], groupIdx) % bucketNum;
            int subbucketHash = HashGenerator.pairwiseIndependent(subbucketHashes[i], coeffIdx) % subbucketNum;
            int itemHash = HashGenerator.fourwiseIndependent(itemHashes[i], products) % 2;
            int sign = itemHash == 1 ? 1 : -1;
            counters[i][groupHash][subbucketHash] += diff * sign;
        }
    }

    // estimate the L2 moment of the vector (sum of squares)
    public double estimateL2(long groupIdx) {
        double estimates[] = new double[testsNum];
        for (int i = 0; i < testsNum; i++) {
            int groupHash = HashGenerator.pairwiseIndependent(this.bucketHashes[i], groupIdx) % bucketNum;
            estimates[i] = 0;
            for (int j = 0; j < subbucketNum; j++) {
                estimates[i] += counters[i][groupHash][j] * counters[i][groupHash][j];
            }
        }
        return getMedian(estimates);
    }

    // estimate value frequency. Can be called only for singleton groups, hence does not need to iterate over subbuckets
    public double estimateValue(long coeffIdx, long groupIdx) {
        long[] products = HashGenerator.productVector(coeffIdx);
        double estimates[] = new double[testsNum];
        for (int i = 0; i < testsNum; i++) {
            int itemHash = HashGenerator.fourwiseIndependent(itemHashes[i], products) % 2;
            int subbucketHash = HashGenerator.pairwiseIndependent(subbucketHashes[i], coeffIdx) % subbucketNum;
            int groupHash = HashGenerator.pairwiseIndependent(bucketHashes[i], groupIdx) % bucketNum;
            if (itemHash == 1) {
                estimates[i] += counters[i][groupHash][subbucketHash];
            } else {
                estimates[i] -= counters[i][groupHash][subbucketHash];
            }
        }
        return getMedian(estimates);
    }

    private static double getMedian(double[] data) {
        return QuickSelect.select(data, data.length / 2);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < testsNum; i++) {
            sb.append("Test #").append(i + 1).append("\n");
            for (int j = 0; j < bucketNum; j++) {
                sb.append("Bucket #").append(j).append(" [ ");
                for (int k = 0; k < subbucketNum; k++) {
                    sb.append(counters[i][j][k]).append("\t");
                }
                sb.append(" ]\n");
            }
        }
        return sb.toString();
    }

    //    public double count(int group, int level) {
    //        int h, f, mult;
    //        double[] estimates = new double[testsNum];
    //
    //        for (int i = 0; i < testsNum; i++) {
    //            h = HashGenerator.pairwiseIndependent(this.pairwiseIndependentHashSeeds[i][0],
    //                    this.pairwiseIndependentHashSeeds[i][1], group);
    //            h = h % (this.bucketNum);
    //
    //            f = HashGenerator.pairwiseIndependent(this.pairwiseIndependentHashSeeds[i][2],
    //                    this.pairwiseIndependentHashSeeds[i][3], group);
    //            f = f % (this.subbucketNum);
    //
    //            mult = HashGenerator.fourwiseIndependent(this.pairwiseIndependentHashSeeds[i][4],
    //                    this.pairwiseIndependentHashSeeds[i][5], this.pairwiseIndependentHashSeeds[i][6],
    //                    this.pairwiseIndependentHashSeeds[i][7], group);
    //            if ((mult & 1) == 1)
    //                estimates[i] += this.counters[level][i][h][f];
    //            else
    //                estimates[i] -= this.counters[level][i][h][f];
    //        }
    //
    //        return getMedian(estimates);
    //    }
}
