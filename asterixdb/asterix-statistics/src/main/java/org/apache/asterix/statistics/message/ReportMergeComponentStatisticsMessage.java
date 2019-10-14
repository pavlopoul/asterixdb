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
package org.apache.asterix.statistics.message;

import java.util.List;

import org.apache.asterix.statistics.StatisticsMetadataUtil;
import org.apache.asterix.statistics.common.StatisticsManager.StatisticsEntry;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class ReportMergeComponentStatisticsMessage extends ReportFlushComponentStatisticsMessage {
    private static final long serialVersionUID = 1L;

    private List<ComponentStatisticsId> mergeComponentIds;

    public ReportMergeComponentStatisticsMessage(StatisticsEntry entry, String node, String partition,
            ComponentStatisticsId newComponentId, boolean isAntimatter,
            List<ComponentStatisticsId> mergeComponentsIds) {
        super(entry, node, partition, newComponentId, isAntimatter);
        this.mergeComponentIds = mergeComponentsIds;
    }

    @Override
    public String toString() {
        return ReportMergeComponentStatisticsMessage.class.getSimpleName();
    }

    @Override
    protected void handleMessage(IMetadataProvider mdProvider) throws AlgebricksException {
        StatisticsMetadataUtil.handleMerge(mdProvider, entry.getDataverse(), entry.getDataset(), entry.getIndex(),
                entry.getField(), node, partition, componentId, isAntimatter, entry.getSynopsis(), mergeComponentIds);
    }
}
