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
package org.apache.hyracks.storage.am.statistics.sketch.groupcount;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

public class GroupCountSketchTest {

    private GroupCountSketch sketch;

    private final HashFunction hashFunctionMock;
    private final long[] hashes;
    private static final int testsNum = 5;
    private static final int bucketNum = 10;
    private static final int subbucketNum = 100;
    private static double epsilon = 0.001;

    public GroupCountSketchTest() {
        hashFunctionMock = mock(HashFunction.class);
        when(hashFunctionMock.hashLong(anyLong())).thenAnswer(new Answer<HashCode>() {
            private long count = 0;

            @Override
            public HashCode answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
                buffer.putLong(count++);
                buffer.putLong(count++);
                return HashCode.fromBytes(buffer.array());
            }
        });
        hashes = new long[] { 4, 5, 6, 7 };
    }

    @Before
    public void init() throws HyracksDataException {
        sketch = new GroupCountSketch(testsNum, bucketNum, subbucketNum, new Random(), hashFunctionMock);
    }

    @Test
    public void testSameGroups() {
        final long coeffIdx = 0L;
        final long groupIdx = 0L;
        sketch.update(coeffIdx, groupIdx, 1.0);
        sketch.update(coeffIdx, groupIdx, 2.0);
        sketch.update(coeffIdx, groupIdx, 3.0);

        long[] products = HashGenerator.productVector(coeffIdx);
        int sign = HashGenerator.fourwiseIndependent(hashes, products) % 2 == 1 ? 1 : -1;
        assertEquals(sketch.estimateValue(groupIdx, groupIdx), sign * 6.0, epsilon);
    }

    @Test
    public void testDifferentGroups() {
        final long coeffIdx = 0L;
        sketch.update(coeffIdx, 0L, 1.0);
        sketch.update(coeffIdx, 1L, 2.0);
        sketch.update(coeffIdx, 2L, 3.0);

        long[] products = HashGenerator.productVector(coeffIdx);
        int sign = HashGenerator.fourwiseIndependent(hashes, products) % 2 == 1 ? 1 : -1;
        assertEquals(sketch.estimateValue(0L, 0L), sign * 1.0, epsilon);
        assertEquals(sketch.estimateValue(0L, 1L), sign * 2.0, epsilon);
        assertEquals(sketch.estimateValue(0L, 2L), sign * 3.0, epsilon);
    }

    //    @Test
    //    public void testDifferentGroups() {
    //        sketch.update(0L, 0L, 1.0);
    //
    //    }
}
