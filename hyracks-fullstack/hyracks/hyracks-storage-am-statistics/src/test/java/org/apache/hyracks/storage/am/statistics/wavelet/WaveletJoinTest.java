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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class WaveletJoinTest extends WaveletTest {

    protected WaveletSynopsis leftSynopsis;
    protected WaveletSynopsis rightSynopsis;
    protected WaveletSynopsis joinedSynopsis;
    protected Collection<WaveletCoefficient> leftCoefficients;
    protected Collection<WaveletCoefficient> rightCoefficients;

    @DataPoints
    public static List<DomainConstants> domains = Arrays.asList(Domain_0_15, Domain_Minus8_7, Domain_Minus15_0);

    public WaveletJoinTest() {
        super(10);
    }

    public void init(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize) {
        leftSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        leftCoefficients = leftSynopsis.getElements();

        rightSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        rightCoefficients = rightSynopsis.getElements();

        joinedSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
    }

    @Theory
    // [1,2,5] ⋈ [2,2,4] = [2,2]
    public void testJoinCoefficients(@FromDataPoints("rawWavelet") WaveletSynopsisSupplier waveletSupplier,
            DomainConstants consts, @FromDataPoints("normalizationOff") Boolean normalize) {
        init(waveletSupplier, consts, normalize);

        // init coefficients for transformed relation [2,2,4]
        leftCoefficients.add(new WaveletCoefficient(0.1875, 5, 0));
        leftCoefficients.add(new WaveletCoefficient(-0.1875, 4, 1));
        leftCoefficients.add(new WaveletCoefficient(0.125, 3, 3));
        leftCoefficients.add(new WaveletCoefficient(-0.5, 2, 6));
        leftCoefficients.add(new WaveletCoefficient(0.25, 2, 7));
        leftCoefficients.add(new WaveletCoefficient(1, 1, 13));
        leftCoefficients.add(new WaveletCoefficient(0.5, 1, 14));
        // init coefficients for transformed relation [1,2,5]
        rightCoefficients.add(new WaveletCoefficient(0.1875, 5, 0));
        rightCoefficients.add(new WaveletCoefficient(-0.1875, 4, 1));
        rightCoefficients.add(new WaveletCoefficient(0.125, 3, 3));
        rightCoefficients.add(new WaveletCoefficient(0.25, 2, 7));
        rightCoefficients.add(new WaveletCoefficient(-0.5, 1, 12));
        rightCoefficients.add(new WaveletCoefficient(0.5, 1, 13));
        rightCoefficients.add(new WaveletCoefficient(-0.5, 1, 14));

        joinedSynopsis.join(leftSynopsis, rightSynopsis);
        joinedSynopsis.orderByKeys();
        PeekingIterator<WaveletCoefficient> it = new PeekingIterator<>(joinedSynopsis.getElements().iterator());

        assertEquals(0.125, joinedSynopsis.findCoeffValue(it, 0, 5), epsilon);
        assertEquals(-0.125, joinedSynopsis.findCoeffValue(it, 1, 4), epsilon);
        assertEquals(0.25, joinedSynopsis.findCoeffValue(it, 3, 3), epsilon);
        assertEquals(-0.5, joinedSynopsis.findCoeffValue(it, 6, 2), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 7, 2), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 12, 1), epsilon);
        assertEquals(1, joinedSynopsis.findCoeffValue(it, 13, 1), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 14, 1), epsilon);
    }
}
