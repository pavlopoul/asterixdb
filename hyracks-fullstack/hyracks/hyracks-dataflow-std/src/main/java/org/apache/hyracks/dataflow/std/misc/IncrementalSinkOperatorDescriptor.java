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
package org.apache.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class IncrementalSinkOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public IncrementalSinkOperatorDescriptor(IOperatorDescriptorRegistry spec) {
        super(spec, 1, 0);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId iId = new ActivityId(getOperatorId(), 0);
        IncrementalActivityNode jc = new IncrementalActivityNode(iId);
        builder.addActivity(this, jc);
        builder.addSourceEdge(0, jc, 0);
    }

    public static class IncrementalTaskState extends AbstractStateObject {

        private IncrementalTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class IncrementalActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public IncrementalActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.open(ctx);

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);

                }

                @Override
                public void fail() throws HyracksDataException {
                    // TODO Auto-generated method stub

                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    ctx.setStateObject(state);

                }

                public MaterializerTaskState getState() {
                    return state;
                }

            };

        }

    }

    //    @Override
    //    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
    //            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
    //        final RecordDescriptor outRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
    //        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(outRecordDesc);
    //        return new AbstractUnaryInputSinkOperatorNodePushable() {
    //            private RunFileWriter runFileWriter;
    //
    //            @Override
    //            public void open() throws HyracksDataException {
    //                FileReference file = ctx.getJobletContext()
    //                        .createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
    //                runFileWriter = new RunFileWriter(file, ctx.getIoManager());
    //                runFileWriter.open();
    //            }
    //
    //            @Override
    //            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
    //                frameTupleAccessor.reset(buffer);
    //                runFileWriter.nextFrame(buffer);
    //
    //            }
    //
    //            @Override
    //            public void fail() throws HyracksDataException {
    //                // TODO Auto-generated method stub
    //
    //            }
    //
    //            @Override
    //            public void close() throws HyracksDataException {
    //                runFileWriter.close();
    //            }
    //
    //        };
    //    }

}
