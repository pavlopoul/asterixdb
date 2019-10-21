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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;

public class StatisticsMessageIOOperationCallbackWrapper implements ILSMIOOperationCallback {
    private final ILSMIOOperationCallback wrapperIOOpCallback;
    private final IStatisticsManager statisticsManager;

    public StatisticsMessageIOOperationCallbackWrapper(ILSMIOOperationCallback wrapperIOOpCallback,
            IStatisticsManager statisticsManager) {
        this.wrapperIOOpCallback = wrapperIOOpCallback;
        this.statisticsManager = statisticsManager;
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        wrapperIOOpCallback.beforeOperation(operation);
    }

    @Override
    public void afterOperation(ILSMIOOperation operation) throws HyracksDataException {
        wrapperIOOpCallback.afterOperation(operation);
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
        wrapperIOOpCallback.afterFinalize(operation);
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH
                || operation.getIOOpertionType() == LSMIOOperationType.LOAD) {
            statisticsManager.sendFlushStatistics(operation.getNewComponent());
        } else if (operation.getIOOpertionType() == LSMIOOperationType.MERGE) {
            statisticsManager.sendMergeStatistics(operation.getNewComponent(),
                    operation.getAccessor().getOpContext().getComponentsToBeMerged());
        }
    }

    @Override
    public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
        wrapperIOOpCallback.recycled(component);
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        wrapperIOOpCallback.allocated(component);
    }

    @Override
    public void scheduled(ILSMIOOperation operation) throws HyracksDataException {
        wrapperIOOpCallback.scheduled(operation);

    }

    @Override
    public void completed(ILSMIOOperation operation) {
        wrapperIOOpCallback.completed(operation);

    }

    public ILSMIOOperationCallback getWrapper() {
        return this.wrapperIOOpCallback;
    }

}
