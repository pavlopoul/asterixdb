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
package org.apache.asterix.translator;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

public class ClientJobRequest extends BaseClientRequest {
    private final JobId jobId;

    public ClientJobRequest(IStatementExecutorContext ctx, String clientCtxId, JobId jobId) {
        super(ctx, clientCtxId);
        this.jobId = jobId;
    }

    @Override
    protected void doCancel(ICcApplicationContext appCtx) throws HyracksDataException {
        IHyracksClientConnection hcc = appCtx.getHcc();
        try {
            JobId[] jobIds = { jobId, null };
            hcc.cancelJob(jobIds);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        ctx.remove(contextId);
    }
}
