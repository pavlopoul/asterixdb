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
package org.apache.hyracks.storage.am.statistics.wavelet.coeff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hyracks.storage.am.statistics.wavelet.DyadicTupleRange;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WaveletCoefficientTest {

    private static int MAX_LEVEL = 3;
    private final long domainStart;
    private final long domainEnd;
    private final long DOMAIN_LENGTH = 8;

    private final WaveletCoefficient input0;
    private final WaveletCoefficient input1;
    private final WaveletCoefficient input2;
    private final WaveletCoefficient input3;
    private final WaveletCoefficient input4;
    private final WaveletCoefficient input5;
    private final WaveletCoefficient input6;
    private final WaveletCoefficient input7;
    private final WaveletCoefficient coeff0;
    private final WaveletCoefficient coeff1;
    private final WaveletCoefficient coeff2;
    private final WaveletCoefficient coeff3;
    private final WaveletCoefficient coeff4;
    private final WaveletCoefficient coeff5;
    private final WaveletCoefficient coeff6;
    private final WaveletCoefficient coeff7;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { 0, 7 }, { -4, 3 }, { -8, 0 } });
    }

    public WaveletCoefficientTest(long domainStart, long domainEnd) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
        this.input0 = new WaveletCoefficient(0.0, 0, domainStart);
        this.input1 = new WaveletCoefficient(0.0, 0, domainStart + 1);
        this.input2 = new WaveletCoefficient(0.0, 0, domainStart + 2);
        this.input3 = new WaveletCoefficient(0.0, 0, domainStart + 3);
        this.input4 = new WaveletCoefficient(0.0, 0, domainStart + 4);
        this.input5 = new WaveletCoefficient(0.0, 0, domainStart + 5);
        this.input6 = new WaveletCoefficient(0.0, 0, domainStart + 6);
        this.input7 = new WaveletCoefficient(0.0, 0, domainStart + 7);
        this.coeff0 = new WaveletCoefficient(0.0, 4, 0);
        this.coeff1 = new WaveletCoefficient(0.0, 3, 1);
        this.coeff2 = new WaveletCoefficient(0.0, 2, 2);
        this.coeff3 = new WaveletCoefficient(0.0, 2, 3);
        this.coeff4 = new WaveletCoefficient(0.0, 1, 4);
        this.coeff5 = new WaveletCoefficient(0.0, 1, 5);
        this.coeff6 = new WaveletCoefficient(0.0, 1, 6);
        this.coeff7 = new WaveletCoefficient(0.0, 1, 7);
    }

    @Test
    public void testGetLevel() {
        assertEquals(MAX_LEVEL + 1, WaveletCoefficient.getLevel(0L, MAX_LEVEL));
        assertEquals(MAX_LEVEL, WaveletCoefficient.getLevel(1L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 1, WaveletCoefficient.getLevel(2L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 1, WaveletCoefficient.getLevel(3L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 2, WaveletCoefficient.getLevel(4L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 2, WaveletCoefficient.getLevel(5L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 2, WaveletCoefficient.getLevel(6L, MAX_LEVEL));
        assertEquals(MAX_LEVEL - 2, WaveletCoefficient.getLevel(7L, MAX_LEVEL));
    }

    @Test
    public void testParentCoeffLevel0() {
        assertEquals(4, input0.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(4, input1.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(5, input2.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(5, input3.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, input4.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, input5.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, input6.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, input7.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testAncestorCoeffLevel0() {
        assertEquals(domainStart, input0.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 1, input1.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 2, input2.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 3, input3.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 4, input4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 5, input5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 6, input6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));
        assertEquals(domainStart + 7, input7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 0));

        assertEquals(4, input0.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(4, input1.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(5, input2.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(5, input3.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(6, input4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(6, input5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(7, input6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));
        assertEquals(7, input7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 1));

        assertEquals(2, input0.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(2, input1.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(2, input2.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(2, input3.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(3, input4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(3, input5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(3, input6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(3, input7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));

        assertEquals(1, input0.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input1.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input2.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input3.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(1, input7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
    }

    @Test
    public void testParentCoeffLevel1() {
        assertEquals(2, coeff4.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(2, coeff5.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(3, coeff6.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(3, coeff7.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testAncestorCoeffLevel1() {
        assertEquals(1, coeff4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(1, coeff5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(1, coeff6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(1, coeff7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));

        assertEquals(0, coeff4.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(0, coeff5.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(0, coeff6.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
        assertEquals(0, coeff7.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 3));
    }

    @Test
    public void testParentCoeffLevel2() {
        assertEquals(1, coeff2.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(1, coeff3.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testAncestorCoeffLevel2() {
        assertEquals(0, coeff2.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
        assertEquals(0, coeff3.getAncestorCoeffIndex(domainStart, MAX_LEVEL, 2));
    }

    @Test
    public void testParentCoeffLevel3() {
        assertEquals(0, coeff1.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testParentCoeffLevel4() {
        assertEquals(-1, coeff0.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel1() {
        assertEquals(domainStart, coeff4.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 2, coeff5.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 4, coeff6.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 6, coeff7.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel2() {
        assertEquals(4, coeff2.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, coeff3.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel3() {
        assertEquals(2, coeff1.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel4() {
        assertEquals(1, coeff0.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffInvalid() {
        assertEquals(-1, input0.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input1.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input2.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input3.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input4.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input5.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input6.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input7.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel1() {
        assertEquals(domainStart + 1, coeff4.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 3, coeff5.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 5, coeff6.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 7, coeff7.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel2() {
        assertEquals(5, coeff2.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, coeff3.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel3() {
        assertEquals(3, coeff1.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel4() {
        assertEquals(1, coeff0.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffInvalid() {
        assertEquals(-1, input0.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input1.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input2.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input3.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input4.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input5.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input6.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, input7.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testCoeffToPositionLevel0() {
        assertEquals(domainStart, input0.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 1, input1.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 2, input2.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 3, input3.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 4, input4.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 5, input5.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 6, input6.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 7, input7.convertCoeffToPosition(domainStart, domainEnd));
    }

    @Test
    public void testCoeffToPositionLevel1() {
        assertEquals(domainStart, coeff4.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 2, coeff5.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 4, coeff6.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 6, coeff7.convertCoeffToPosition(domainStart, domainEnd));
    }

    @Test
    public void testCoeffToPositionLevel2() {
        assertEquals(domainStart, coeff2.convertCoeffToPosition(domainStart, domainEnd));
        assertEquals(domainStart + 4, coeff3.convertCoeffToPosition(domainStart, domainEnd));
    }

    @Test
    public void testCoeffToPositionLevel3() {
        assertEquals(domainStart, coeff1.convertCoeffToPosition(domainStart, domainEnd));
    }

    @Test
    public void testSupportIntervalLevel0() {
        assertEquals(new DyadicTupleRange(domainStart, domainStart, 0.0),
                input0.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 1, domainStart + 1, 0.0),
                input1.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 2, domainStart + 2, 0.0),
                input2.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 3, domainStart + 3, 0.0),
                input3.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 4, 0.0),
                input4.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 5, domainStart + 5, 0.0),
                input5.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 6, domainStart + 6, 0.0),
                input6.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 7, domainStart + 7, 0.0),
                input7.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
    }

    @Test
    public void testSupportIntervalLevel1() {
        assertEquals(new DyadicTupleRange(domainStart, domainStart + 1, 0.0),
                coeff4.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 2, domainStart + 3, 0.0),
                coeff5.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 5, 0.0),
                coeff6.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 6, domainStart + 7, 0.0),
                coeff7.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
    }

    @Test
    public void testSupportIntervalLevel2() {
        assertEquals(new DyadicTupleRange(domainStart, domainStart + 3, 0.0),
                coeff2.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 7, 0.0),
                coeff3.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
    }

    @Test
    public void testSupportIntervalLevel3() {
        assertEquals(new DyadicTupleRange(domainStart, domainEnd, 0.0),
                coeff1.convertCoeffToSupportInterval(domainStart, domainEnd, MAX_LEVEL));
    }

    @Test
    public void testCoversLevel0() {
        assertTrue(input0.covers(domainStart, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 1, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 2, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 3, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 4, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 5, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 6, MAX_LEVEL, domainStart));
        assertFalse(input0.covers(domainStart + 7, MAX_LEVEL, domainStart));
    }

    @Test
    public void testCoversLevel1() {
        assertTrue(coeff4.covers(domainStart, MAX_LEVEL, domainStart));
        assertTrue(coeff4.covers(domainStart + 1, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 2, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 3, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 4, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 5, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 6, MAX_LEVEL, domainStart));
        assertFalse(coeff4.covers(domainStart + 7, MAX_LEVEL, domainStart));
    }

    @Test
    public void testCoversLevel2() {
        assertTrue(coeff2.covers(domainStart, MAX_LEVEL, domainStart));
        assertTrue(coeff2.covers(domainStart + 1, MAX_LEVEL, domainStart));
        assertTrue(coeff2.covers(domainStart + 2, MAX_LEVEL, domainStart));
        assertTrue(coeff2.covers(domainStart + 3, MAX_LEVEL, domainStart));
        assertFalse(coeff2.covers(domainStart + 4, MAX_LEVEL, domainStart));
        assertFalse(coeff2.covers(domainStart + 5, MAX_LEVEL, domainStart));
        assertFalse(coeff2.covers(domainStart + 6, MAX_LEVEL, domainStart));
        assertFalse(coeff2.covers(domainStart + 7, MAX_LEVEL, domainStart));
    }

    @Test
    public void testCoversLevel3() {
        assertTrue(coeff1.covers(domainStart, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 1, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 2, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 3, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 4, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 5, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 6, MAX_LEVEL, domainStart));
        assertTrue(coeff1.covers(domainStart + 7, MAX_LEVEL, domainStart));
    }

    @Test
    public void testCoversLevel4() {
        assertTrue(coeff0.covers(domainStart, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 1, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 2, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 3, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 4, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 5, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 6, MAX_LEVEL, domainStart));
        assertTrue(coeff0.covers(domainStart + 7, MAX_LEVEL, domainStart));
    }
}
