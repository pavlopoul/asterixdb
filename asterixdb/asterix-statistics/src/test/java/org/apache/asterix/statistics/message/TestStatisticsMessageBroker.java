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

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.statistics.TestMetadataProvider;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.messages.IMessage;

public class TestStatisticsMessageBroker implements INCMessageBroker {

    private final TestMetadataProvider mdProvider;

    public TestStatisticsMessageBroker(TestMetadataProvider mdProvider) {
        this.mdProvider = mdProvider;
    }

    @Override
    public void sendMessageToPrimaryCC(ICcAddressedMessage message) throws Exception {
        ReportFlushComponentStatisticsMessage statsMsg = (ReportFlushComponentStatisticsMessage) message;
        statsMsg.handleMessage(mdProvider);
    }

    @Override
    public void sendMessageToCC(CcId ccId, ICcAddressedMessage message) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendMessageToNC(String nodeId, INcAddressedMessage message) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queueReceivedMessage(INcAddressedMessage msg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageFuture registerMessageFuture() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageFuture deregisterMessageFuture(long futureId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        throw new UnsupportedOperationException();
    }
}
