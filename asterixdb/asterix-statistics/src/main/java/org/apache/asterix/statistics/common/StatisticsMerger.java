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
import java.util.TimerTask;

import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.MetadataConstants;

public class StatisticsMerger extends TimerTask {

    @Override
    public void run() {
        MetadataTransactionContext mdTxnCtx = null;
        boolean bActiveTxn = false;
        try {
            MetadataManager.INSTANCE.init();
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dv : dataverses) {
                //skip Metadata dataverse, since stats are not collected for it anyway
                if (dv.getDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {
                    continue;
                }
                List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dv.getDataverseName());
                for (Dataset ds : datasets) {
                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dv.getDataverseName(),
                            ds.getDatasetName());
                    for (Index idx : indexes) {
                        MetadataManager.INSTANCE.getMergedStatistics(mdTxnCtx, dv.getDataverseName(),
                                ds.getDatasetName(), idx.getIndexName(),
                                String.join(".", idx.getKeyFieldNames().get(0)));
                    }
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (Exception e) {
            try {
                if (bActiveTxn) {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                }
            } catch (Exception ex) {
                System.err.println("Metadata transaction abort failed:" + ex.getMessage());
            }
            System.err.println("Metadata operation failed:" + e.getMessage());
        }
    }
}
