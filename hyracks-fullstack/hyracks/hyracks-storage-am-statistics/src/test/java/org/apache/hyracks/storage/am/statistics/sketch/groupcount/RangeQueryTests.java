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

import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.test.support.RepeatRule;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.math.DoubleMath;

@RunWith(Parameterized.class)
public class RangeQueryTests extends GroupCountSketchBuilderTest {
    public static long DOMAIN_START = 0L;
    public static long DOMAIN_END = 15L;
    public static int MAX_LEVEL = 4;
    public static int SIZE = 100;
    public static int INPUT_SIZE = 3;

    private Function<Double, Matcher<Double>> probabilisticMatcherGenerator;
    private Map<Double, List<Matcher<Double>>> matchers = new HashMap<>();
    private Map<Double, Integer> matcherIndexes = new HashMap<>();

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(new Object[] { 2, 4, 16 });
    }

    public RangeQueryTests(int fanout) {
        super(DOMAIN_START, DOMAIN_END, MAX_LEVEL, SIZE, false, fanout, INPUT_SIZE);
        probabilisticMatcherGenerator = (Double expectedValue) -> new BaseMatcher<Double>() {
            private int permittedNumberOfFailures = FAILURE_NUM;

            @Override
            public void describeTo(Description description) {
            }

            @Override
            public boolean matches(Object item) {
                if (!(item instanceof Double)) {
                    return false;
                }
                boolean isMatched = DoubleMath.fuzzyEquals(expectedValue, (Double) item, ACCURACY);
                if (isMatched) {
                    return true;
                }
                if (permittedNumberOfFailures-- > 0) {
                    return true;
                }
                return false;
            }
        };
    }

    private Matcher<Double> getMatcher(Double expectedResult) {
        List<Matcher<Double>> matcherList = matchers.computeIfAbsent(expectedResult, (x) -> {
            matcherIndexes.put(x, 0);
            return new ArrayList<>();
        });
        Matcher<Double> result;
        int currMatcherIndex = matcherIndexes.get(expectedResult);
        if (currMatcherIndex >= matcherList.size()) {
            result = probabilisticMatcherGenerator.apply(expectedResult);
            matcherList.add(result);
        } else {
            result = matcherList.get(currMatcherIndex);
        }
        matcherIndexes.replace(expectedResult, ++currMatcherIndex);
        return result;
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = FAILURE_TRIES)
    public void testSameValue() throws HyracksDataException {
        builder.addValue(0L);
        builder.addValue(0L);
        builder.addValue(0L);

        builder.end();

        //reset matcher indexes
        matcherIndexes.replaceAll((val, index) -> 0);

        assertThat(synopsis.rangeQuery(DOMAIN_START, DOMAIN_END), getMatcher(3.0));
        assertThat(synopsis.rangeQuery(0L, 1L), getMatcher(3.0));
        assertThat(synopsis.pointQuery(0), getMatcher(3.0));
    }

    @Test
    @Repeat(times = FAILURE_TRIES)
    public void testDifferentValues() throws HyracksDataException {
        builder.addValue(0L);
        builder.addValue(1L);
        builder.addValue(2L);

        builder.end();

        //reset matcher indexes
        matcherIndexes.replaceAll((val, index) -> 0);

        assertThat(synopsis.rangeQuery(DOMAIN_START, DOMAIN_END), getMatcher(3.0));
        assertThat(synopsis.rangeQuery(0L, 2L), getMatcher(3.0));
        assertThat(synopsis.pointQuery(0L), getMatcher(1.0));
        assertThat(synopsis.pointQuery(1L), getMatcher(1.0));
        assertThat(synopsis.pointQuery(2L), getMatcher(1.0));
    }

    @Test
    @Repeat(times = FAILURE_TRIES)
    public void testDifferentValuesUnordered() throws HyracksDataException {
        builder.addValue(2L);
        builder.addValue(0L);
        builder.addValue(1L);

        builder.end();

        //reset matcher indexes
        matcherIndexes.replaceAll((val, index) -> 0);

        assertThat(synopsis.rangeQuery(DOMAIN_START, DOMAIN_END), getMatcher(3.0));
        assertThat(synopsis.rangeQuery(0L, 2L), getMatcher(3.0));
        assertThat(synopsis.pointQuery(0L), getMatcher(1.0));
        assertThat(synopsis.pointQuery(1L), getMatcher(1.0));
        assertThat(synopsis.pointQuery(2L), getMatcher(1.0));
    }
}
