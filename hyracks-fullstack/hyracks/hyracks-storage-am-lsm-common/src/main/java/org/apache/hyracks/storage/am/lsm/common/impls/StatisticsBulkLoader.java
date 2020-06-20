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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class StatisticsBulkLoader implements IChainedComponentBulkLoader {

    protected final ISynopsisBuilder statisticsBuilder;
    private final AbstractLSMDiskComponent component;
    private final LSMIOOperationType opType;
    protected final IStatisticsManager statisticsManager;

    public StatisticsBulkLoader(ISynopsisBuilder statisticsBuilder, IStatisticsManager statisticsManager,
            AbstractLSMDiskComponent component, LSMIOOperationType opType) {
        this.statisticsBuilder = statisticsBuilder;
        this.statisticsManager = statisticsManager;
        this.component = component;
        this.opType = opType;
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        statisticsBuilder.add(tuple);
        return tuple;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        statisticsBuilder.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        statisticsBuilder.end();
        statisticsBuilder.gatherComponentStatistics(statisticsManager, component, opType);
        component.getStatistics().writeTuplesNum(component.getMetadata());
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean hasFailed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Throwable getFailure() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}
