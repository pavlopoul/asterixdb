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
package org.apache.asterix.statistics;

import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class StatisticsMetadataUtil {
    private static final Logger LOGGER = Logger.getLogger(StatisticsMetadataUtil.class.getName());

    private StatisticsMetadataUtil() {
    }

    public static void handleMerge(IMetadataProvider mdProvider, String dataverseName, String datasetName,
            String indexName, String fieldName, String node, String partition, ComponentStatisticsId newComponentId,
            boolean isAntimatter, ISynopsis synopsis, List<ComponentStatisticsId> mergeComponentIds)
            throws AlgebricksException {
        // delete old stats. Even if the new stats are empty we need to invalidate earlier synopses
        for (ComponentStatisticsId mergedComponentId : mergeComponentIds) {
            LOGGER.fine("Deleting old stat with componentId " + mergedComponentId);
            mdProvider.dropStatistics(dataverseName, datasetName, indexName, fieldName, node, partition,
                    mergedComponentId, isAntimatter);
        }
        // insert statistics on merged component only if synopsis is not empty, i.e. skip empty statistics
        if (synopsis != null) {
            LOGGER.fine("Adding new stat with componentId " + newComponentId);
            mdProvider.addStatistics(dataverseName, datasetName, indexName, fieldName, node, partition, newComponentId,
                    isAntimatter, synopsis);
        }
    }

    public static void handleFlush(IMetadataProvider mdProvider, String dataverseName, String datasetName,
            String indexName, String fieldName, String node, String partition, ComponentStatisticsId componentId,
            boolean isAntimatter, ISynopsis synopsis) throws AlgebricksException {
        if (synopsis != null) {
            LOGGER.fine("Adding new stat with componentId " + componentId);
            mdProvider.addStatistics(dataverseName, datasetName, indexName, fieldName, node, partition, componentId,
                    isAntimatter, synopsis);
        }
    }
}
