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

import java.util.Arrays;
import java.util.Collection;

import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WaveletCoefficientOverflowTest {

    private final long domainStart;
    private final long domainEnd;
    private final int maxLevel;

    private final long minIndex;
    private final long maxIndex;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { Long.MIN_VALUE, Long.MAX_VALUE, Long.SIZE },
                { Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.SIZE },
                { Short.MIN_VALUE, Short.MAX_VALUE, Short.SIZE }, { Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.SIZE } });
    }

    public WaveletCoefficientOverflowTest(long domainStart, long domainEnd, int maxLevel) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
        this.maxLevel = maxLevel;
        // init axillary coefficient values
        this.minIndex = 1L << (maxLevel - 1);
        this.maxIndex = minIndex | (minIndex - 1);
    }

    @Test
    public void testParentCoeffLevel0Overflow() {
        WaveletCoefficient underflowCoeff = new WaveletCoefficient(0.0, 0, domainStart);
        WaveletCoefficient overflowCoeff = new WaveletCoefficient(0.0, 0, domainEnd);

        assertEquals(minIndex, underflowCoeff.getParentCoeffIndex(domainStart, maxLevel));
        assertEquals(maxIndex, overflowCoeff.getParentCoeffIndex(domainStart, maxLevel));
    }

    @Test
    public void testLeftChildCoeffLevel0Overflow() {
        WaveletCoefficient underflowCoeff = new WaveletCoefficient(0.0, 1, minIndex);
        WaveletCoefficient overflowCoeff = new WaveletCoefficient(0.0, 1, maxIndex);

        assertEquals(domainStart, underflowCoeff.getLeftChildCoeffIndex(domainStart, maxLevel));
        assertEquals(domainEnd - 1, overflowCoeff.getLeftChildCoeffIndex(domainStart, maxLevel));
    }

    @Test
    public void testRightChildCoeffLevel0Overflow() {
        WaveletCoefficient underflowCoeff = new WaveletCoefficient(0.0, 1, minIndex);
        WaveletCoefficient overflowCoeff = new WaveletCoefficient(0.0, 1, maxIndex);

        assertEquals(domainStart + 1, underflowCoeff.getRightChildCoeffIndex(domainStart, maxLevel));
        assertEquals(domainEnd, overflowCoeff.getRightChildCoeffIndex(domainStart, maxLevel));

    }

    @Test
    public void testGetLevelOverflow() {
        assertEquals(1, WaveletCoefficient.getLevel(minIndex, maxLevel));
        assertEquals(1, WaveletCoefficient.getLevel(maxIndex, maxLevel));
    }
}
