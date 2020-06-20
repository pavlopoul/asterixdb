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
package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class CombinedSynopsisBuilder implements ISynopsisBuilder {

    private final ISynopsisBuilder synopsisBuilder;
    private final ISynopsisBuilder antimatterSynopsisBuilder;

    public CombinedSynopsisBuilder(ISynopsisBuilder synopsisBuilder, ISynopsisBuilder antimatterSynopsisBuilder) {
        this.synopsisBuilder = synopsisBuilder;
        this.antimatterSynopsisBuilder = antimatterSynopsisBuilder;
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        if (((ILSMTreeTupleReference) tuple).isAntimatter()) {
            antimatterSynopsisBuilder.add(tuple);
        } else {
            synopsisBuilder.add(tuple);
        }
    }

    @Override
    public void end() throws HyracksDataException {
        synopsisBuilder.end();
        antimatterSynopsisBuilder.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        synopsisBuilder.abort();
        antimatterSynopsisBuilder.abort();
    }

    @Override
    public void gatherComponentStatistics(IStatisticsManager statisticsManager, ILSMDiskComponent component,
            LSMIOOperationType opType) throws HyracksDataException {
        synopsisBuilder.gatherComponentStatistics(statisticsManager, component, opType);
        antimatterSynopsisBuilder.gatherComponentStatistics(statisticsManager, component, opType);
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
