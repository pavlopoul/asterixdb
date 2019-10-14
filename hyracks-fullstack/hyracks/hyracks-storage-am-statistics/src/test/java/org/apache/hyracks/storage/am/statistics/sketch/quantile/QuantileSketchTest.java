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
package org.apache.hyracks.storage.am.statistics.sketch.quantile;

import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.text.RandomStringGenerator;
import org.apache.hyracks.test.support.RepeatRule;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class QuantileSketchTest<T extends Comparable<T>> {

    private final static double ACCURACY = 0.001;
    private static int NUM_RECORDS = 5000;
    private static int QUANTILE_NUM = 20;
    private final Function<Random, T> randomGenerator;
    private final T domainMin;

    private QuantileSketch<T> sketch;

    public QuantileSketchTest(Function<Random, T> randomGenerator, T domainMin) {
        this.randomGenerator = randomGenerator;
        this.domainMin = domainMin;
    }

    @Parameters
    public static Collection<Object[]> data() {
        Function<Random, Byte> byteGenerator = (Random r) -> (byte) (r.nextInt(256) - 128);
        Function<Random, Byte> shortGenerator = (Random r) -> (byte) (r.nextInt(65536) - 32768);
        Function<Random, Integer> intGenerator = Random::nextInt;
        Function<Random, Long> longGenerator = Random::nextLong;
        Function<Random, String> stringGenerator = (Random r) -> new RandomStringGenerator.Builder()
                .withinRange('a', 'z').usingRandom(r::nextInt).build().generate(10);
        return Arrays.asList(new Object[][] { { byteGenerator, Byte.MIN_VALUE }, { shortGenerator, Short.MIN_VALUE },
                { intGenerator, Integer.MIN_VALUE }, { longGenerator, Long.MIN_VALUE }, { stringGenerator, "" } });
    }

    private void init(List<T> inputData) {
        sketch = new QuantileSketch<>(QUANTILE_NUM, domainMin, ACCURACY);
        for (T i : inputData) {
            sketch.insert(i);
        }
    }

    private List<T> generateRandomData(int numRecords) {
        List<T> data = new ArrayList<>(numRecords);
        Random r = new Random(System.nanoTime());
        for (int i = 0; i < numRecords; i++) {
            data.add(randomGenerator.apply(r));
        }
        return data;
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = 100)
    public void testQuantileCorrectness() {
        List<T> inputData = generateRandomData(NUM_RECORDS);
        init(inputData);
        Collections.sort(inputData);
        List<T> approximateRanks = sketch.finish();
        //        Collections.sort(inputData);
        assertEquals(QUANTILE_NUM, approximateRanks.size());
        for (int i = 1; i <= QUANTILE_NUM; i++) {
            // inner loop for all ε-accurate values of the rank
            Set<T> rankValues = new HashSet<>();
            //rankValues.add(domainMax);
            double rank = ((double) i) / QUANTILE_NUM;
            int remainder = 0;
            if (rank == 1) {
                remainder = 5;
            }
            for (int j = Math.max(0, (int) Math.floor((rank - ACCURACY) * NUM_RECORDS) - remainder); j < Math
                    .min(NUM_RECORDS, (int) Math.floor((rank + ACCURACY) * NUM_RECORDS)); j++) {
                rankValues.add(inputData.get(j));
            }
            assertThat(approximateRanks.get(i - 1), isIn(rankValues));
        }
    }
}
