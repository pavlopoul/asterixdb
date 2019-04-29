/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.translator;

import org.apache.asterix.common.api.IClientRequest;

/**
 * The context for statement executors. Maintains ongoing user requests.
 */
public interface IStatementExecutorContext {

    /**
     * Gets the client request from the user-provided client context id.
     *
     * @param clientContextId,
     *            a user provided client context id.
     * @return the client request
     */
    IClientRequest get(String clientContextId);

    /**
     * Puts a client context id for a statement and the corresponding request.
     *
     * @param clientContextId,
     *            a user provided client context id.
     * @param req,
     *            the Hyracks job id of class {@link org.apache.hyracks.api.job.JobId}.
     */
    void put(String clientContextId, IClientRequest req);

    /**
     * Removes the information about the query corresponding to a user-provided client context id.
     *
     * @param clientContextId,
     *            a user provided client context id.
     */
    IClientRequest remove(String clientContextId);
}