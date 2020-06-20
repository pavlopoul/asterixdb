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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_STATISTICS;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.statistics.StatisticsMetadataUtil;
import org.apache.asterix.statistics.common.StatisticsManager.StatisticsEntry;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class ReportFlushComponentStatisticsMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ReportFlushComponentStatisticsMessage.class.getName());

    protected StatisticsEntry entry;
    protected String node;
    protected String partition;
    protected boolean isAntimatter;
    protected ComponentStatisticsId componentId;

    public ReportFlushComponentStatisticsMessage(StatisticsEntry entry, String node, String partition,
            ComponentStatisticsId componentId, boolean isAntimatter) {
        this.entry = entry;
        this.node = node;
        this.partition = partition;
        this.componentId = componentId;
        this.isAntimatter = isAntimatter;
    }

    public StatisticsEntry getStatisticsEntry() {
        return entry;
    }

    public String getNode() {
        return node;
    }

    public String getPartition() {
        return partition;
    }

    public boolean isAntimatter() {
        return isAntimatter;
    }

    public ComponentStatisticsId getComponentId() {
        return componentId;
    }

    @Override
    public String toString() {
        return ReportFlushComponentStatisticsMessage.class.getSimpleName();
    }

    @Override
    public void handle(ICcApplicationContext cs) throws HyracksDataException, InterruptedException {
        LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "] received statistics message " + entry
                + " from node=" + node + ", partition=" + partition + ", componentId=" + componentId);
        MetadataProvider mdProvider = MetadataProvider.create(cs, null);
        boolean bActiveTxn = false;
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // transactionally update metadata with received statistics
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            MetadataLockUtil.insertStatisticsBegin(cs.getMetadataLockManager(), mdProvider.getLocks(),
                    PROPERTIES_STATISTICS.getDatasetName(), DataverseName.createFromCanonicalForm(entry.getDataverse()),
                    entry.getDataset(), entry.getIndex(), entry.getField(), node, partition, isAntimatter);

            handleMessage(mdProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (AlgebricksException | ACIDException | RemoteException me) {
            if (bActiveTxn) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } catch (ACIDException | RemoteException e) {
                    throw HyracksDataException.create(e);
                }
            }
            throw HyracksDataException.create(me);
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    void handleMessage(IMetadataProvider mdProvider) throws AlgebricksException {
        StatisticsMetadataUtil.handleFlush(mdProvider, entry.getDataverse(), entry.getDataset(), entry.getIndex(),
                entry.getField(), node, partition, componentId, isAntimatter, entry.getSynopsis());
    }
}
