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
public class TransformRangeQueryTests extends WaveletTransformTest {

    private Random rand;

    public TransformRangeQueryTests() {
        super(Integer.MAX_VALUE);
        rand = new Random();
    }

    @DataPoints
    public static List<DomainConstants> domains = Arrays.asList(Domain_0_15, Domain_Minus8_7, Domain_Minus15_0);

    @Theory
    public void EmptyPointTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList();
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 14), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 15), epsilon);
    }

    @Theory
    public void SinglePointDomainStartTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Collections.singletonList(new TransformTuple(consts.domainStart, 4));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 1), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 2), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 3), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 4), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 5), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 6), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 7), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 8), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 9), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 11), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 12), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 13), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 14), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 15), epsilon);

        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 14), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 15), epsilon);
    }

    @Theory
    public void SinglePointDomainEndTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Collections.singletonList(new TransformTuple(consts.domainEnd, 4));
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 14), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 15), epsilon);

        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 2, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 3, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 4, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 5, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 6, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 7, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 8, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 9, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 10, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 11, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 12, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 13, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 14, consts.domainEnd), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 15, consts.domainEnd), epsilon);
    }

    @Theory
    public void TwoPointsTest(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 4),
                new TransformTuple(consts.domainStart + 11, 2));
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 8, consts.domainStart + 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(consts.domainStart + 12, consts.domainStart + 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 6, consts.domainStart + 8), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 5, consts.domainStart + 9), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 4, consts.domainStart + 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 3, consts.domainStart + 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 2, consts.domainStart + 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart + 1, consts.domainStart + 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(consts.domainStart, consts.domainStart + 10), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(consts.domainStart + 8, consts.domainStart + 15), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(consts.domainStart + 9, consts.domainStart + 14), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(consts.domainStart + 10, consts.domainStart + 13), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(consts.domainStart + 11, consts.domainStart + 12), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(consts.domainStart + 7, consts.domainStart + 11), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(consts.domainStart + 6, consts.domainStart + 12), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(consts.domainStart + 5, consts.domainStart + 13), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(consts.domainStart + 4, consts.domainStart + 14), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(consts.domainStart + 3, consts.domainStart + 15), epsilon);
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
            for (long j = i; j <= consts.domainEnd; j++) {
                int val = i <= randomPoint && randomPoint <= j ? 1 : 0;
                Assert.assertEquals(val, synopsis.rangeQuery(i, j), epsilon);
            }
        }
    }
}
