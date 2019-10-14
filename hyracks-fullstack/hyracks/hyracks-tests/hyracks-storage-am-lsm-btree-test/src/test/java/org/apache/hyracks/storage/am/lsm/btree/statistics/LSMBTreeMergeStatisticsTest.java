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
package org.apache.hyracks.storage.am.lsm.btree.statistics;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestStatisticsManager;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMBTreeMergeStatisticsTest extends StatisticsTestDriver {

    private final OrderedIndexTestUtils orderedIndexTestUtils = new OrderedIndexTestUtils();

    protected static final int numInsertRounds = 3;
    protected static final int numTuplesToInsertPerRound = numTuplesToInsert / numInsertRounds;

    public LSMBTreeMergeStatisticsTest() {
        super(LSMBTreeTestHarness.LEAF_FRAMES_TO_TEST);
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType, false);
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
        ctx.getIndex().create();
        ctx.getIndex().activate();
        for (int i = 0; i < numInsertRounds; i++) {
            for (int j = 0; j < i; j++) {
                if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
                    orderedIndexTestUtils.insertIntTuples(ctx, numTuplesToInsertPerRound, getRandom());
                } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
                    orderedIndexTestUtils.insertStringTuples(ctx, numTuplesToInsertPerRound, getRandom());
                }
                //force flush operation
                accessor.scheduleFlush();
            }
        }
        ctx.getIndex().validate();

        accessor.scheduleMerge(((LSMBTree) ctx.getIndex()).getDiskComponents());
        for (int i = 0; i < fieldSerdes.length; i++) {
            checkStatistics((TestStatisticsManager) harness.getStatisticsManager(),
                    new ArrayList<>(ctx.getCheckTuples()), 1, i);
        }
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "Merge Statistics";
    }
}
