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
package org.apache.asterix.statistics.common;

import java.util.List;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.ICardinalityEstimator;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

import net.agkn.hll.util.HLLUtil;
import net.agkn.hll.util.LongIterator;

public class CardinalityEstimator implements ICardinalityEstimator {

    public static CardinalityEstimator INSTANCE = new CardinalityEstimator();

    private boolean primIndex;

    private long estimationTime;

    private List<Statistics> statistics;

    private CardinalityEstimator() {
    }

    @Override
    public long getRangeCardinality(IMetadataProvider metadataProvider, String dataverseName, String datasetName,
            List<String> fieldName, long rangeStart, long rangeStop) throws AlgebricksException {

        List<Statistics> stats = null;
        List<Index> datasetIndexes =
                ((MetadataProvider) metadataProvider).getDatasetIndexes(dataverseName, datasetName);
        for (Index idx : datasetIndexes) {
            // TODO : allow statistics on nested fields
            List<Statistics> fieldStats = ((MetadataProvider) metadataProvider).getMergedStatistics(dataverseName,
                    datasetName, idx.getIndexName(), String.join(".", fieldName));
            // use the last if multiple stats on the same field are available
            if (!fieldStats.isEmpty()) {
                stats = fieldStats;
            }
        }
        if (stats == null || stats.isEmpty()) {
            return CardinalityInferenceVisitor.UNKNOWN;
        }

        long startTime = System.nanoTime();
        double estimate = 0.0;

        for (Statistics s : stats) {
            double synopsisEstimate = 0.0;
            if (rangeStart < rangeStop) {
                synopsisEstimate = s.getSynopsis().rangeQuery(rangeStart, rangeStop);
            } else if (rangeStart == rangeStop) {
                synopsisEstimate = s.getSynopsis().pointQuery(rangeStart);
            }
            estimate += synopsisEstimate * (s.isAntimatter() ? -1 : 1);
        }
        long endTime = System.nanoTime();
        estimationTime = endTime - startTime;
        if (estimate < 0) {
            return 0L;
        }
        return Math.round(estimate);
    }

    @Override
    public long getTableCardinality(IMetadataProvider metadataProvider, String dataverseName, String datasetName,
            List<String> fieldName) throws AlgebricksException {
        statistics = getFieldStats(metadataProvider, dataverseName, datasetName, fieldName);
        if (statistics == null)
            return 0;
        long result = 0l;
        for (Statistics s : statistics) {
            result += s.getSynopsis().joinQuery(s.getSynopsis(), this.primIndex);
        }
        return (long) Math.ceil(result);
    }

    @Override
    public long getJoinCardinality(IMetadataProvider metadataProvider, String innerDataverseName,
            String innerDatasetName, List<String> innerFieldName, String outerDataverseName, String outerDatasetName,
            List<String> outerFieldName) throws AlgebricksException {
        long result = getTableCardinality(metadataProvider, innerDataverseName, innerDatasetName, innerFieldName);
        long innerUniqueValues = 0;
        long outerUniqueValues = 0;
        if (statistics != null) {
            innerUniqueValues = getUniqueCardinality(metadataProvider, statistics);
            //            if (!primIndex) {
            //                innerUniqueValues /= ((MetadataProvider) metadataProvider).getClusterLocations().getLocations().length;
            //            }
        }

        long resultout = getTableCardinality(metadataProvider, outerDataverseName, outerDatasetName, outerFieldName);
        if (result == 0 || resultout == 0)
            return 0;
        if (statistics != null) {
            outerUniqueValues = getUniqueCardinality(metadataProvider, statistics);
            //            if (!primIndex) {
            //                outerUniqueValues /= ((MetadataProvider) metadataProvider).getClusterLocations().getLocations().length;
            //            }
        }

        System.out.println(result + ", " + resultout);
        System.out.println(innerUniqueValues + ", " + outerUniqueValues);
        //        if (innerDatasetName.equals("Store_Sales") && outerDatasetName.equals("d13")) {
        //            result /= 2;
        //        }
        //        if (innerDatasetName.equals("ss5") && outerDatasetName.equals("sr4")) {
        //            result /= 2;
        //        }
        return Math.max(1, result) * Math.max(1, resultout) / Math.max(innerUniqueValues, outerUniqueValues);
    }

    @Override
    public long getJoinAfterFilterCardinality(IMetadataProvider metadataProvider, String innerDataverseName,
            String innerDatasetName, List<String> innerFieldName, String outerDataverseName, String outerDatasetName,
            List<String> outerFieldName, long result) throws AlgebricksException {
        long innerUniqueValues = result;
        long outerUniqueValues = 0;
        statistics = getFieldStats(metadataProvider, innerDataverseName, innerDatasetName, innerFieldName);
        if (statistics != null) {
            innerUniqueValues = getUniqueCardinality(metadataProvider, statistics);
            if (!primIndex) {
                innerUniqueValues /= ((MetadataProvider) metadataProvider).getClusterLocations().getLocations().length;
            }
        }

        long resultout = getTableCardinality(metadataProvider, outerDataverseName, outerDatasetName, outerFieldName);
        if (result == 0 || resultout == 0)
            return 0;
        if (statistics != null) {
            outerUniqueValues = getUniqueCardinality(metadataProvider, statistics);
            if (!primIndex) {
                outerUniqueValues /= ((MetadataProvider) metadataProvider).getClusterLocations().getLocations().length;
            }
        }

        System.out.println(result + ", " + resultout);
        System.out.println(innerUniqueValues + ", " + outerUniqueValues);
        return Math.max(1, result) * Math.max(1, resultout) / Math.max(innerUniqueValues, outerUniqueValues);
    }

    private List<Statistics> getFieldStats(IMetadataProvider metadataProvider, String dataverseName, String datasetName,
            List<String> fieldName) throws AlgebricksException {
        List<Statistics> stats = null;
        List<Index> datasetIndexes =
                ((MetadataProvider) metadataProvider).getDatasetIndexes(dataverseName, datasetName);
        for (Index idx : datasetIndexes) {

            // TODO : allow statistics on nested fields
            List<Statistics> fieldStats = ((MetadataProvider) metadataProvider).getMergedStatistics(dataverseName,
                    datasetName, idx.getIndexName(), String.join(".", fieldName));
            // use the last if multiple stats on the same field are available
            if (!fieldStats.isEmpty()) {
                stats = fieldStats;
                if (!idx.getKeyFieldNames().isEmpty() && idx.getKeyFieldNames().size() < 2
                        && idx.getKeyFieldNames().get(0).get(0).equals(fieldName.get(0))) {
                    //if (idx.isPrimaryIndex()) {
                    this.primIndex = true;
                } else {
                    this.primIndex = false;
                }
            }
        }
        return stats;
    }

    public LongIterator registerIterator(long[] words) {
        return new LongIterator() {
            final int registerWidth = 5;
            //   final long[] words = BitVector.this.words;
            final long registerMask = 31;

            // register setup
            long registerIndex = 0;
            int wordIndex = 0;
            int remainingWordBits = 64;
            long word = words[wordIndex];

            @Override
            public long next() {
                long register;
                if (remainingWordBits >= registerWidth) {
                    register = word & registerMask;

                    // shift to the next register
                    word >>>= registerWidth;
                    remainingWordBits -= registerWidth;
                } else { /*insufficient bits remaining in current word*/
                    wordIndex++/*move to the next word*/;

                    register = (word | (words[wordIndex] << remainingWordBits)) & registerMask;

                    // shift to the next partial register (word)
                    word = words[wordIndex] >>> (registerWidth - remainingWordBits);
                    remainingWordBits += 64 - registerWidth;
                }
                registerIndex++;
                return register;
            }

            @Override
            public boolean hasNext() {
                return registerIndex < 1048576;
            }
        };
    }

    public long getUniqueCardinality(IMetadataProvider mp, List<Statistics> stats) throws AlgebricksException {
        ISynopsis<? extends ISynopsisElement<Long>> synopsis = null;
        double alphaMSquared = HLLUtil.alphaMSquared(1048576);
        double smallEstimatorCutoff = HLLUtil.smallEstimatorCutoff(1048576);
        double largeEstimatorCutoff = HLLUtil.largeEstimatorCutoff(20, 5);
        if (stats.get(0).getNode().equals("")) {
            synopsis = stats.get(0).getSynopsis();
        } else {
            synopsis = stats.get(stats.size() - 1).getSynopsis();
        }
        if (synopsis.getUnique() != null && !synopsis.getUnique().isEmpty()) {
            return synopsis.getUnique().size();
        } else if (synopsis.getSparseMap() != null && !synopsis.getSparseMap().isEmpty()) {
            double sum = 0;
            int numberOfZeroes = 0/*"V" in the paper*/;
            for (int j = 0; j < 1048576; j++) {
                final long register = synopsis.getSparseMap().getOrDefault(j, (byte) 0);

                sum += 1.0 / (1L << register);
                if (register == 0L)
                    numberOfZeroes++;
            }

            final double estimator = alphaMSquared / sum;
            if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
                return (long) HLLUtil.smallEstimator(1048576, numberOfZeroes);
            } else if (estimator <= largeEstimatorCutoff) {
                return (long) estimator;
            } else {
                return (long) HLLUtil.largeEstimator(20, 5, estimator);
            }
        } else {

            double sum = 0;
            int numberOfZeroes = 0/*"V" in the paper*/;
            final LongIterator iterator = registerIterator(synopsis.getWordsAr());
            while (iterator.hasNext()) {
                final long register = iterator.next();

                sum += 1.0 / (1L << register);
                if (register == 0L)
                    numberOfZeroes++;
            }
            final double estimator = alphaMSquared / sum;
            if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
                return (long) HLLUtil.smallEstimator(1048576, numberOfZeroes);
            } else if (estimator <= largeEstimatorCutoff) {
                return (long) estimator;
            } else {
                return (long) HLLUtil.largeEstimator(20, 5, estimator);
            }
        }

        //long estimate = 1;

        //        Set<Long> set = stats.get(0).getSynopsis().getUnique();
        //        if (set.isEmpty()) {
        //            Map<Integer, Byte> sparseMap = stats.get(0).getSynopsis().getSparseMap();
        //            for (int i = 1; i < stats.size(); i++) {
        //
        //                if (!stats.get(i).getSynopsis().getUnique().isEmpty()) {
        //                    set = stats.get(i).getSynopsis().getUnique();
        //                    for (long rawValue : set) {
        //                        final long substreamValue = (rawValue >>> 15);
        //                        final byte p_w;
        //
        //                        if (substreamValue == 0L) {
        //                            p_w = 0;
        //                        } else {
        //                            p_w = (byte) (1 + BitUtil.leastSignificantBit(substreamValue | HLLUtil.pwMaxMask(5)));
        //                        }
        //
        //                        final int j = (int) (rawValue & 32767);
        //
        //                        final byte currentValue = sparseMap.getOrDefault(j, (byte) 0);
        //                        if (p_w > currentValue) {
        //                            sparseMap.put(j, p_w);
        //                        }
        //                    }
        //                    stats.get(i).getSynopsis().getUnique().clear();
        //                    Statistics merged = new Statistics(stats.get(i).getDataverseName(), stats.get(i).getDatasetName(),
        //                            stats.get(i).getIndexName(), stats.get(i).getFieldName(), stats.get(i).getNode() + "n",
        //                            stats.get(i).getPartition() + "n", stats.get(i).getComponentID(), true, false,
        //                            stats.get(i).getSynopsis());
        //                    ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //                } else if (!stats.get(i).getSynopsis().getSparseMap().isEmpty()) {
        //                    for (final int registerIndex : stats.get(i).getSynopsis().getSparseMap().keySet()) {
        //                        final byte registerValue = stats.get(i).getSynopsis().getSparseMap().get(registerIndex);
        //                        final byte currentRegisterValue = sparseMap.getOrDefault(registerIndex, (byte) 0);
        //                        if (registerValue > currentRegisterValue) {
        //                            sparseMap.put(registerIndex, registerValue);
        //                        }
        //                    }
        //                    stats.get(i).getSynopsis().getSparseMap().clear();
        //                    Statistics merged = new Statistics(stats.get(i).getDataverseName(), stats.get(i).getDatasetName(),
        //                            stats.get(i).getIndexName(), stats.get(i).getFieldName(), stats.get(i).getNode() + "n",
        //                            stats.get(i).getPartition() + "n", stats.get(i).getComponentID(), true, false,
        //                            stats.get(i).getSynopsis());
        //                    ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //                }
        //
        //            }
        //            int m = 1 << 15;
        //            if (sparseMap.size() > 8192 || sparseMap.size() == 0) {
        //
        //                long words[] = new long[491583 >>> 6]/*for convenience/performance*/;
        //                for (final int registerIndex : sparseMap.keySet()) {
        //                    final byte value = sparseMap.get(registerIndex);
        //                    final long bitIndex = registerIndex * 5;
        //                    final int firstWordIndex = (int) (bitIndex >>> 6)/*aka (bitIndex / BITS_PER_WORD)*/;
        //                    final int secondWordIndex = (int) ((bitIndex + 4) >>> 6)/*see above*/;
        //                    final int bitRemainder = (int) (bitIndex & 63)/*aka (bitIndex % BITS_PER_WORD)*/;
        //
        //                    // NOTE:  matches getRegister()
        //                    final long registerValue;
        //
        //                    if (firstWordIndex == secondWordIndex)
        //                        registerValue = ((words[firstWordIndex] >>> bitRemainder) & 31);
        //                    else /*register spans words*/
        //                        registerValue = (words[firstWordIndex] >>> bitRemainder)/*no need to mask since at top of word*/
        //                                | (words[secondWordIndex] << (64 - bitRemainder)) & 31;
        //
        //                    // determine which is the larger and update as necessary
        //                    if (value > registerValue) {
        //                        // NOTE:  matches setRegister()
        //                        if (firstWordIndex == secondWordIndex) {
        //                            // clear then set
        //                            words[firstWordIndex] &= ~(31 << bitRemainder);
        //                            words[firstWordIndex] |= (value << bitRemainder);
        //                        } else {/*register spans words*/
        //                            // clear then set each partial word
        //                            words[firstWordIndex] &= (1L << bitRemainder) - 1;
        //                            words[firstWordIndex] |= (value << bitRemainder);
        //
        //                            words[secondWordIndex] &= ~(31 >>> (64 - bitRemainder));
        //                            words[secondWordIndex] |= (value >>> (64 - bitRemainder));
        //                        }
        //                    } /* else -- the register value is greater (or equal) so nothing needs to be done */
        //
        //                }
        //                if (sparseMap.isEmpty()) {
        //                    words = stats.get(0).getSynopsis().getWordsAr();
        //                    for (int j = 1; j < stats.size(); j++) {
        //                        for (int i = 0; i < m; i++) {
        //                            final long bitIndex = i * 5;
        //                            final int firstWordIndex = (int) (bitIndex >>> 6)/*aka (bitIndex / BITS_PER_WORD)*/;
        //                            final int secondWordIndex = (int) ((bitIndex + 4) >>> 6)/*see above*/;
        //                            final int bitRemainder = (int) (bitIndex & 63)/*aka (bitIndex % BITS_PER_WORD)*/;
        //                            long registerValue;
        //                            if (firstWordIndex == secondWordIndex)
        //                                registerValue =
        //                                        ((stats.get(j).getSynopsis().getWordsAr()[firstWordIndex] >>> bitRemainder)
        //                                                & 31);
        //                            /* else -- register spans words */
        //                            registerValue = (stats.get(j).getSynopsis()
        //                                    .getWordsAr()[firstWordIndex] >>> bitRemainder)/*no need to mask since at top of word*/
        //                                    | (stats.get(j).getSynopsis().getWordsAr()[secondWordIndex] << (64 - bitRemainder))
        //                                            & 31;
        //                            setMaxRegister(i, registerValue, words);
        //                        }
        //                        stats.get(j).getSynopsis().setWordsAr(null);
        //                        Statistics merged = new Statistics(stats.get(j).getDataverseName(),
        //                                stats.get(j).getDatasetName(), stats.get(j).getIndexName(), stats.get(j).getFieldName(),
        //                                stats.get(j).getNode() + "n", stats.get(j).getPartition() + "n",
        //                                stats.get(j).getComponentID(), true, false, stats.get(j).getSynopsis());
        //                        ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //
        //                    }
        //                }
        //
        //                double sum = 0;
        //                int numberOfZeroes = 0/*"V" in the paper*/;
        //                final LongIterator iterator = registerIterator(words);
        //                while (iterator.hasNext()) {
        //                    final long register = iterator.next();
        //
        //                    sum += 1.0 / (1L << register);
        //                    if (register == 0L)
        //                        numberOfZeroes++;
        //                }
        //
        //                // apply the estimate and correction to the indicator function
        //
        //                double alphaMSquared = (0.7213 / (1.0 + 1.079 / m)) * m * m;
        //                // apply the estimate and correction to the indicator function
        //                final double estimator = alphaMSquared / sum;
        //                double smallEstimatorCutoff = ((double) m * 5) / 2;
        //
        //                double largeEstimatorCutoff = (Math.pow(2, 45)) / 30.0;
        //                stats.get(0).getSynopsis().setWordsAr(words);
        //                Statistics merged = new Statistics(stats.get(0).getDataverseName(), stats.get(0).getDatasetName(),
        //                        stats.get(0).getIndexName(), stats.get(0).getFieldName(), stats.get(0).getNode() + "n",
        //                        stats.get(0).getPartition() + "n", stats.get(0).getComponentID(), true, false,
        //                        stats.get(0).getSynopsis());
        //                ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //                if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
        //                    return (long) (m * Math.log((double) m / numberOfZeroes));
        //                } else if (estimator <= largeEstimatorCutoff) {
        //                    return (long) estimator;
        //                } else {
        //                    return (long) (-1 * Math.pow(2, 45) * Math.log(1.0 - (estimator / Math.pow(2, 45))));
        //                }
        //            }
        //
        //            double sum = 0;
        //            int numberOfZeroes = 0/*"V" in the paper*/;
        //            for (int j = 0; j < m; j++) {
        //                final long register = sparseMap.getOrDefault(j, (byte) 0);
        //
        //                sum += 1.0 / (1L << register);
        //                if (register == 0L)
        //                    numberOfZeroes++;
        //            }
        //            double alphaMSquared = (0.7213 / (1.0 + 1.079 / m)) * m * m;
        //            // apply the estimate and correction to the indicator function
        //            final double estimator = alphaMSquared / sum;
        //            double smallEstimatorCutoff = ((double) m * 5) / 2;
        //
        //            double largeEstimatorCutoff = (Math.pow(2, 45)) / 30.0;
        //            stats.get(0).getSynopsis().getSparseMap().putAll(sparseMap);
        //            Statistics merged = new Statistics(stats.get(0).getDataverseName(), stats.get(0).getDatasetName(),
        //                    stats.get(0).getIndexName(), stats.get(0).getFieldName(), stats.get(0).getNode() + "n",
        //                    stats.get(0).getPartition() + "n", stats.get(0).getComponentID(), true, false,
        //                    stats.get(0).getSynopsis());
        //            ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //            if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
        //                return (long) (m * Math.log((double) m / numberOfZeroes));
        //            } else if (estimator <= largeEstimatorCutoff) {
        //                return (long) estimator;
        //            } else {
        //                return (long) (-1 * Math.pow(2, 45) * Math.log(1.0 - (estimator / Math.pow(2, 45))));
        //            }
        //        }
        //
        //        for (int i = 1; i < stats.size(); i++) {
        //            for (long value : stats.get(i).getSynopsis().getUnique()) {
        //                set.add(value);
        //            }
        //            stats.get(i).getSynopsis().getUnique().clear();
        //            Statistics merged = new Statistics(stats.get(i).getDataverseName(), stats.get(i).getDatasetName(),
        //                    stats.get(i).getIndexName(), stats.get(i).getFieldName(), stats.get(i).getNode() + "n",
        //                    stats.get(i).getPartition() + "n", stats.get(i).getComponentID(), true, false,
        //                    stats.get(i).getSynopsis());
        //            ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //
        //        }
        //        stats.get(0).getSynopsis().getUnique().addAll(set);
        //        Statistics merged = new Statistics(stats.get(0).getDataverseName(), stats.get(0).getDatasetName(),
        //                stats.get(0).getIndexName(), stats.get(0).getFieldName(), stats.get(0).getNode() + "n",
        //                stats.get(0).getPartition() + "n", stats.get(0).getComponentID(), true, false,
        //                stats.get(0).getSynopsis());
        //        ((MetadataProvider) mp).getCache().addStatisticsIfNotExists(merged);
        //
        //        int i = 0;
        //        for (Statistics s : stats) {
        //            if (!s.getSynopsis().getElements().isEmpty()) {
        //                estimate = estimate + s.getSynopsis().uniqueQuery(this.primIndex);
        //                i++;
        //            }
        //        }
        //        if (!primIndex) {
        //            estimate /= i;
        //        }
        //        return Math.round(estimate);
        //        return Math.round(set.size());
    }

    public boolean setMaxRegister(final long registerIndex, final long value, long[] words) {
        final long bitIndex = registerIndex * 5;
        final int firstWordIndex = (int) (bitIndex >>> 6)/*aka (bitIndex / BITS_PER_WORD)*/;
        final int secondWordIndex = (int) ((bitIndex + 4) >>> 6)/*see above*/;
        final int bitRemainder = (int) (bitIndex & 63)/*aka (bitIndex % BITS_PER_WORD)*/;

        // NOTE:  matches getRegister()
        final long registerValue;
        if (firstWordIndex == secondWordIndex)
            registerValue = ((words[firstWordIndex] >>> bitRemainder) & 31);
        else /*register spans words*/
            registerValue = (words[firstWordIndex] >>> bitRemainder)/*no need to mask since at top of word*/
                    | (words[secondWordIndex] << (64 - bitRemainder)) & 31;

        // determine which is the larger and update as necessary
        if (value > registerValue) {
            // NOTE:  matches setRegister()
            if (firstWordIndex == secondWordIndex) {
                // clear then set
                words[firstWordIndex] &= ~(31 << bitRemainder);
                words[firstWordIndex] |= (value << bitRemainder);
            } else {/*register spans words*/
                // clear then set each partial word
                words[firstWordIndex] &= (1L << bitRemainder) - 1;
                words[firstWordIndex] |= (value << bitRemainder);

                words[secondWordIndex] &= ~(31 >>> (64 - bitRemainder));
                words[secondWordIndex] |= (value >>> (64 - bitRemainder));
            }
        } /* else -- the register value is greater (or equal) so nothing needs to be done */

        return (value >= registerValue);
    }

    @Override
    public long getEstimationTime() {
        return estimationTime;
    }

}
