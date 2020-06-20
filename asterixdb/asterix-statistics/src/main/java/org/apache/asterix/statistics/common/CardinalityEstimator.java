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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.ICardinalityEstimator;

public class CardinalityEstimator implements ICardinalityEstimator {

    public static CardinalityEstimator INSTANCE = new CardinalityEstimator();

    private long estimationTime;

    private CardinalityEstimator() {
    }

    @Override
    public long getRangeCardinality(IMetadataProvider metadataProvider, String dataverseName, String datasetName,
            List<String> fieldName, long rangeStart, long rangeStop) throws AlgebricksException {

        List<Statistics> stats = null;
        List<Index> datasetIndexes = ((MetadataProvider) metadataProvider)
                .getDatasetIndexes(DataverseName.createFromCanonicalForm(dataverseName), datasetName);
        for (Index idx : datasetIndexes) {
            // TODO : allow statistics on nested fields
            List<Statistics> fieldStats = ((MetadataProvider) metadataProvider).getMergedStatistics(
                    DataverseName.createFromCanonicalForm(dataverseName), datasetName, idx.getIndexName(),
                    String.join(".", fieldName));
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
    public long getJoinCardinality(IMetadataProvider metadataProvider, String innerDataverseName,
            String innerDatasetName, List<String> innerFieldName, String outerDataverseName, String outerDatasetName,
            List<String> outerFieldName) {
        return CardinalityInferenceVisitor.UNKNOWN;
    }

    @Override
    public long getEstimationTime() {
        return estimationTime;
    }

}
