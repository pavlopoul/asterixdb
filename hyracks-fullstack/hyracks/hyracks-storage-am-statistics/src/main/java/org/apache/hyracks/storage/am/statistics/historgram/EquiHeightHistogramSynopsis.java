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
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;

import net.agkn.hll.util.BitUtil;
import net.agkn.hll.util.HLLUtil;
import net.agkn.hll.util.NumberUtil;

public abstract class EquiHeightHistogramSynopsis<T extends HistogramBucket> extends HistogramSynopsis<T> {

    private static final long serialVersionUID = 1L;
    private final long elementsPerBucket;

    private Set<Long> set;

    public EquiHeightHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum, int bucketsNum,
            List<T> buckets, Map<Long, Integer> uniqueMap, Set<Long> uniqueSet, Map<Integer, Byte> map, long[] words) {
        super(domainStart, domainEnd, maxLevel, bucketsNum, buckets, uniqueMap, uniqueSet, map, words);
        elementsPerBucket = Math.max((long) Math.ceil((double) elementsNum / bucketsNum), 1);
    }

    public long getElementsPerBucket() {
        return elementsPerBucket;
    }

    @Override
    public void merge(ISynopsis<T> mergeSynopsis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mergeUnique(ISynopsis<T> mergeSynopsis) {
        set = getUnique();
        if (!mergeSynopsis.getUnique().isEmpty()) {
            for (long value : mergeSynopsis.getUnique()) {
                if (getUnique() != null) {
                    getUnique().add(value);
                    if (getUnique().size() > 81920) {
                        for (final long rawvalue : getUnique()) {
                            addRawSparseProbabilistic(rawvalue);
                        }
                        set = null;
                        setUniqueSet(set);
                    }
                } else {
                    addRawSparseProbabilistic(value);
                }
            }
        } else if (!mergeSynopsis.getSparseMap().isEmpty()) {
            if (getSparseMap() != null) {
                for (final int registerIndex : mergeSynopsis.getSparseMap().keySet()) {
                    final byte registerValue = mergeSynopsis.getSparseMap().get(registerIndex);
                    final byte currentRegisterValue = getSparseMap().getOrDefault(registerIndex, (byte) 0);
                    if (registerValue > currentRegisterValue) {
                        getSparseMap().put(registerIndex, registerValue);
                    }
                }
                final int largestPow2LessThanCutoff = (int) NumberUtil.log2((1048576 * 5) / 25);
                int sparseThreshold = (1 << largestPow2LessThanCutoff);
                if (getSparseMap().size() > sparseThreshold) {
                    for (final int registerIndex : getSparseMap().keySet()) {
                        final byte registerValue = getSparseMap().get(registerIndex);
                        setMaxRegister(registerIndex, registerValue);
                    }
                    setSparseMap(null);
                }
            } else {
                for (final int registerIndex : mergeSynopsis.getSparseMap().keySet()) {
                    final byte registerValue = mergeSynopsis.getSparseMap().get(registerIndex);
                    setMaxRegister(registerIndex, registerValue);
                }
            }
        } else if (mergeSynopsis.getWordsAr() != null) {
            for (int j = 0; j < 1048576; j++) {
                final long registerValue = getRegister(mergeSynopsis.getWordsAr(), j);
                setMaxRegister(j, registerValue);
            }
        }
        mergeSynopsis.getUnique().clear();
        mergeSynopsis.getSparseMap().clear();
        mergeSynopsis.setWordsAr(null);

    }

    public long getRegister(long[] words, final long registerIndex) {
        final long bitIndex = registerIndex * 5;
        final int firstWordIndex = (int) (bitIndex >>> 6)/* aka (bitIndex / BITS_PER_WORD) */;
        final int secondWordIndex = (int) ((bitIndex + 4) >>> 6)/* see above */;
        final int bitRemainder = (int) (bitIndex & 63)/* aka (bitIndex % BITS_PER_WORD) */;

        if (firstWordIndex == secondWordIndex)
            return ((words[firstWordIndex] >>> bitRemainder) & 31);
        /* else -- register spans words */
        return (words[firstWordIndex] >>> bitRemainder)/* no need to mask since at top of word */
                | (words[secondWordIndex] << (64 - bitRemainder)) & 31;
    }

    public boolean setMaxRegister(final long registerIndex, final long value) {
        final long bitIndex = registerIndex * 5;
        final int firstWordIndex = (int) (bitIndex >>> 6)/* aka (bitIndex / BITS_PER_WORD) */;
        final int secondWordIndex = (int) ((bitIndex + 4) >>> 6)/* see above */;
        final int bitRemainder = (int) (bitIndex & 63)/* aka (bitIndex % BITS_PER_WORD) */;

        // NOTE: matches getRegister()
        final long registerValue;
        final long words[] = getWordsAr();/* for convenience/performance */;
        if (firstWordIndex == secondWordIndex)
            registerValue = ((words[firstWordIndex] >>> bitRemainder) & 31);
        else /* register spans words */
            registerValue = (words[firstWordIndex] >>> bitRemainder)/* no need to mask since at top of word */
                    | (words[secondWordIndex] << (64 - bitRemainder)) & 31;

        // determine which is the larger and update as necessary
        if (value > registerValue) {
            // NOTE: matches setRegister()
            if (firstWordIndex == secondWordIndex) {
                // clear then set
                words[firstWordIndex] &= ~(31 << bitRemainder);
                words[firstWordIndex] |= (value << bitRemainder);
            } else {/* register spans words */
                // clear then set each partial word
                words[firstWordIndex] &= (1L << bitRemainder) - 1;
                words[firstWordIndex] |= (value << bitRemainder);

                words[secondWordIndex] &= ~(31 >>> (64 - bitRemainder));
                words[secondWordIndex] |= (value >>> (64 - bitRemainder));
            }
        } /*
          * else -- the register value is greater (or equal) so nothing needs to be done
          */

        return (value >= registerValue);
    }

    private void addRawSparseProbabilistic(final long rawValue) {
        final long substreamValue = (rawValue >>> 20);
        final byte p_w;

        if (substreamValue == 0L) {
            p_w = 0;
        } else {
            p_w = (byte) (1 + BitUtil.leastSignificantBit(substreamValue | HLLUtil.pwMaxMask(5)));
        }

        if (p_w == 0) {
            return;
        }

        final int j = (int) (rawValue & 1048575);

        final byte currentValue = getSparseMap().getOrDefault(j, (byte) 0);
        if (p_w > currentValue) {
            getSparseMap().put(j, p_w);
        }

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
