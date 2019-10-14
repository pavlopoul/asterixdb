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

import java.util.PriorityQueue;

import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.test.support.RepeatRule;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoint;

public abstract class WaveletTest {
    protected final int threshold;

    protected static double epsilon = 0.001;

    public static class DomainConstants {
        protected final long domainStart;
        protected final long domainEnd;
        protected final int maxLevel;

        public DomainConstants(long domainStart, long domainEnd, int maxLevel) {
            this.domainStart = domainStart;
            this.domainEnd = domainEnd;
            this.maxLevel = maxLevel;
        }
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    protected static DomainConstants Domain_0_15 = new DomainConstants(0, 15, 4);
    protected static DomainConstants Domain_Minus15_0 = new DomainConstants(-15, 0, 4);
    protected static DomainConstants Domain_Minus8_7 = new DomainConstants(-8, 7, 4);
    protected static DomainConstants Domain_Long = new DomainConstants(Long.MIN_VALUE, Long.MAX_VALUE, Long.SIZE);
    protected static DomainConstants Domain_Integer =
            new DomainConstants(Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.SIZE);
    protected static DomainConstants Domain_Short = new DomainConstants(Short.MIN_VALUE, Short.MAX_VALUE, Short.SIZE);
    protected static DomainConstants Domain_Byte = new DomainConstants(Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.SIZE);

    interface WaveletSynopsisSupplier {

        WaveletSynopsis createSynopsis(DomainConstants domain, int threshold, boolean normalize);

        AbstractIntegerSynopsisBuilder createSynopsisBuilder(WaveletSynopsis synopsis);
    }

    @DataPoint("prefixSumWavelet")
    public static WaveletSynopsisSupplier prefixSumWaveletSupplier = new WaveletSynopsisSupplier() {
        @Override
        public WaveletSynopsis createSynopsis(DomainConstants domain, int threshold, boolean normalize) {
            return new PrefixSumWaveletSynopsis(domain.domainStart, domain.domainEnd, domain.maxLevel, threshold,
                    new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        }

        @Override
        public AbstractIntegerSynopsisBuilder createSynopsisBuilder(WaveletSynopsis synopsis) {
            return new PrefixSumWaveletTransform(synopsis, "", "", "", "", false, null, null);
        }
    };

    @DataPoint("rawWavelet")
    public static WaveletSynopsisSupplier rawWaveletSupplier = new WaveletSynopsisSupplier() {
        @Override
        public WaveletSynopsis createSynopsis(DomainConstants domain, int threshold, boolean normalize) {
            return new WaveletSynopsis(domain.domainStart, domain.domainEnd, domain.maxLevel, threshold,
                    new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        }

        @Override
        public AbstractIntegerSynopsisBuilder createSynopsisBuilder(WaveletSynopsis synopsis) {
            return new WaveletTransform(synopsis, "", "", "", "", false, null, null);
        }
    };

    @DataPoint("normalizationOn")
    public static boolean normalizationOn = true;

    @DataPoint("normalizationOff")
    public static boolean normalizationOff = false;

    public WaveletTest(int threshold) {
        this.threshold = threshold;
    }
}
