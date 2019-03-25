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
package org.apache.asterix.external.operators;

import java.util.Set;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.IOperatorEnvironment;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.IncrementalSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;

public class ReaderJobOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private IncrementalSinkOperatorDescriptor sink;

    public ReaderJobOperatorDescriptor(IOperatorDescriptorRegistry spec, IncrementalSinkOperatorDescriptor sink,
            RecordDescriptor rDesc) {
        super(spec, 0, 1);
        this.outRecDescs[0] = rDesc;
        this.sink = sink;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ReaderActivityNode ra = new ReaderActivityNode(new ActivityId(odId, 1));
        builder.addActivity(this, ra);
        builder.addTargetEdge(0, ra, 0);
    }

    private final class ReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions,
                IOperatorEnvironment pastEnv) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    Set<Object> set = pastEnv.getStateObjectKeys();
                    TaskId tId = null;
                    for (Object ob : set) {
                        tId = (TaskId) ob;
                        if (tId.getPartition() == partition) {
                            MaterializerTaskState state = (MaterializerTaskState) pastEnv.getStateObject(tId);
                            state.writeOut(writer, new VSizeFrame(ctx), false);
                        }
                    }
                    // TaskId tId = (TaskId) set.iterator().next();

                    //state.writeOut(writer, new VSizeFrame(ctx), false);
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }

}

//AbstractSingleActivityOperatorDescriptor {
//    private static final long serialVersionUID = 1L;
//
//    public ReaderJobOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc) {
//        super(spec, 0, 1);
//        this.outRecDescs[0] = rDesc;
//    }
//
//    @Override
//    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
//            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
//        return new ReaderJobOperatorNodePushable(ctx, this) {
//
//        };
//    }
//
//}
