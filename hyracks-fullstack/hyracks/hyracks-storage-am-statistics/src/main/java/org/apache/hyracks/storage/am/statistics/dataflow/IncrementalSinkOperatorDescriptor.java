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

package org.apache.hyracks.storage.am.statistics.dataflow;

import java.nio.ByteBuffer;

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
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManagerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.StatisticsFactory;

public class IncrementalSinkOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private StatisticsFactory statisticsFactory;
    private RecordDescriptor recDesc;
    private IStatisticsManagerProvider statsManagerProvider;

    public IncrementalSinkOperatorDescriptor(IOperatorDescriptorRegistry spec, StatisticsFactory statisticsFactory,
            RecordDescriptor recDesc, IStatisticsManagerProvider statsManagerProvider) {
        super(spec, 1, 0);
        this.statisticsFactory = statisticsFactory;
        this.recDesc = recDesc;
        this.statsManagerProvider = statsManagerProvider;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId iId = new ActivityId(getOperatorId(), 0);
        IncrementalActivityNode jc = new IncrementalActivityNode(iId);
        builder.addActivity(this, jc);
        builder.addSourceEdge(0, jc, 0);
    }

    public void setRecDesc(RecordDescriptor recDesc) {
        this.recDesc = recDesc;
    }

    public void setStats(StatisticsFactory statisticsFactory) {
        this.statisticsFactory = statisticsFactory;
    }

    public void setManagerProvider(IStatisticsManagerProvider statsManagerProvider) {
        this.statsManagerProvider = statsManagerProvider;
    }

    private class IncrementalActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public IncrementalActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions,
                IOperatorEnvironment pastEnv) throws HyracksDataException {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;
                protected FrameTupleAccessor accessor;
                protected final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
                protected ISynopsisBuilder builder;
                protected ComponentStatistics component;

                @Override
                public void open() throws HyracksDataException {
                    accessor = new FrameTupleAccessor(recDesc);
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.open(ctx);
                    int[] fieldPermutation = new int[1];
                    fieldPermutation[0] = 0;
                    tuple.setFieldPermutation(fieldPermutation);
                    component = new ComponentStatistics(100l, 100l);

                    builder =
                            IncrementalSinkOperatorDescriptor.this.statisticsFactory.createStatistics(component, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();

                    for (int i = 0; i < tupleCount; i++) {
                        tuple.reset(accessor, i);
                        builder.add(tuple);
                    }

                }

                @Override
                public void fail() throws HyracksDataException {

                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    if (builder != null) {
                        builder.end();
                        builder.gatherIntermediateStatistics(
                                statsManagerProvider.getStatisticsManager(ctx.getJobletContext().getServiceContext()),
                                component, partition);

                    }

                    ctx.setStateObject(state);

                }

            };

        }

    }

}
