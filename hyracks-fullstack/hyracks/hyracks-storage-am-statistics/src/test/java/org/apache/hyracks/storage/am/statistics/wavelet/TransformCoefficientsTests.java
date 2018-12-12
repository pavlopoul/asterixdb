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
import java.util.List;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class TransformCoefficientsTests extends WaveletTransformTest {

    public TransformCoefficientsTests() {
        super(Integer.MAX_VALUE);
    }

    @DataPoints
    public static List<DomainConstants> domains = Arrays.asList(Domain_0_15, Domain_Minus8_7, Domain_Minus15_0);

    @Theory
    public void IncreasingLevelTestUpperBoarder(
            @FromDataPoints("prefixSumWavelet") WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            @FromDataPoints("normalizationOff") Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart, 4),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 12, 3));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(5.75, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals((-1.75), synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals((-1.5), synopsis.findCoeffValue(it, 3L, 3), epsilon);
    }

    @Theory
    public void IncreasingLevelTestLowerBoarder(
            @FromDataPoints("prefixSumWavelet") WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            @FromDataPoints("normalizationOff") Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 4),
                new TransformTuple(consts.domainStart + 11, 2), new TransformTuple(consts.domainStart + 13, 3));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(3.4375, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-2.9375, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-1.875, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 6L, 2), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 13L, 1), epsilon);
        Assert.assertEquals(-0.75, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 14L, 1), epsilon);
    }

    @Theory
    public void IncreasingLevelTestMixedBoarder(
            @FromDataPoints("prefixSumWavelet") WaveletSynopsisSupplier waveletSupplier, DomainConstants consts,
            @FromDataPoints("normalizationOff") Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 4),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 13, 3));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(3.8125, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-3.3125, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-1.125, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-0.75, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 14L, 1), epsilon);
    }

    @Theory
    public void DecreasingLevelTest(@FromDataPoints("prefixSumWavelet") WaveletSynopsisSupplier waveletSupplier,
            DomainConstants consts, @FromDataPoints("normalizationOff") Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(consts.domainStart, 2), new TransformTuple(consts.domainStart + 2, 4),
                        new TransformTuple(consts.domainStart + 4, 6), new TransformTuple(consts.domainStart + 8, 8));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(14, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-6, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-4, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 4L, 2), epsilon);
    }

    @Theory
    public void MixedLevelTest(@FromDataPoints("prefixSumWavelet") WaveletSynopsisSupplier waveletSupplier,
            DomainConstants consts, @FromDataPoints("normalizationOff") Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(consts.domainStart + 7, 8),
                new TransformTuple(consts.domainStart + 8, 2), new TransformTuple(consts.domainStart + 11, 4),
                new TransformTuple(consts.domainEnd, 6));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(7.125, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-6.125, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-4, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-2.25, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 6L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-3, synopsis.findCoeffValue(it, 15L, 1), epsilon);
    }
}
