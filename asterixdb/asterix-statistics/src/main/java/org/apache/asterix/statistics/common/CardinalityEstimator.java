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
        long innerUniqueValues = getUniqueCardinality(statistics);

        long resultout = getTableCardinality(metadataProvider, outerDataverseName, outerDatasetName, outerFieldName);

        long outerUniqueValues = getUniqueCardinality(statistics);

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

    //@Override
    //    public long getUniqueCardinality(IMetadataProvider metadataProvider, String dataverseName, String datasetName,
    //            List<String> fieldName) throws AlgebricksException {
    public long getUniqueCardinality(List<Statistics> stats) throws AlgebricksException {

        long estimate = 1;

        for (Statistics s : stats) {
            estimate = estimate + s.getSynopsis().uniqueQuery(this.primIndex);
        }
        return Math.round(estimate);
    }

    @Override
    public long getEstimationTime() {
        return estimationTime;
    }

}
