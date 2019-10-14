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
package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class TransformPointQueryTests extends WaveletTransformTest {

    private Random rand;

    public TransformPointQueryTests() {
        super(Integer.MAX_VALUE);
        rand = new Random();
    }

    @DataPoints
    public static List<DomainConstants> domains = Arrays.asList(Domain_0_15, Domain_Minus8_7, Domain_Minus15_0);

    @Theory
    public void IncreasingLevelTestUpperBoarder(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart, 4),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 12, 3));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(consts.domainStart), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(consts.domainStart + 8), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(consts.domainStart + 12), epsilon);
    }

    @Theory
    public void IncreasingLevelTestLowerBoarder(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 4),
                new TransformTuple(consts.domainStart + 11, 2), new TransformTuple(consts.domainStart + 13, 3));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(consts.domainStart + 7), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(consts.domainStart + 11), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(consts.domainStart + 13), epsilon);
    }

    @Theory
    public void IncreasingLevelTestMixedBoarder(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 4),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 13, 3));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(consts.domainStart + 7), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(consts.domainStart + 8), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(consts.domainStart + 13), epsilon);
    }

    @Theory
    public void DecreasingLevelTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(consts.domainStart, 2), new TransformTuple(consts.domainStart + 2, 4),
                        new TransformTuple(consts.domainStart + 4, 6), new TransformTuple(consts.domainStart + 8, 8));
        runTest(initialData);

        Assert.assertEquals(2.0, synopsis.pointQuery(consts.domainStart), epsilon);
        Assert.assertEquals(4.0, synopsis.pointQuery(consts.domainStart + 2), epsilon);
        Assert.assertEquals(6.0, synopsis.pointQuery(consts.domainStart + 4), epsilon);
        Assert.assertEquals(8.0, synopsis.pointQuery(consts.domainStart + 8), epsilon);
    }

    @Theory
    public void MixedLevelTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 8),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 11, 4),
                new TransformTuple(consts.domainStart + 15, 6));
        runTest(initialData);

        Assert.assertEquals(8.0, synopsis.pointQuery(consts.domainStart + 7), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(consts.domainStart + 8), epsilon);
        Assert.assertEquals(4.0, synopsis.pointQuery(consts.domainStart + 11), epsilon);
        Assert.assertEquals(6.0, synopsis.pointQuery(consts.domainStart + 15), epsilon);
    }

    @Theory
    @Repeat(times = 100)
    public void RandomPointTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        init(waveletSupplier, consts, normalize);
        long randomPoint = consts.domainStart + rand.nextInt((int) consts.domainEnd - (int) consts.domainStart);
        List<TransformTuple> initialData = Collections.singletonList(new TransformTuple(randomPoint, 1));
        runTest(initialData);

        for (long i = consts.domainStart; i <= consts.domainEnd; i++) {
            int val = i == randomPoint ? 1 : 0;
            Assert.assertEquals(val, synopsis.pointQuery(i), epsilon);
        }
    }
}
