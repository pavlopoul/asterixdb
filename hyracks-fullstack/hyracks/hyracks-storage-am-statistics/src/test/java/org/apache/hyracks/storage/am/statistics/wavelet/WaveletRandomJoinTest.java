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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformHelper;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class WaveletRandomJoinTest extends WaveletTest {

    private WaveletSynopsis leftSynopsis;
    private WaveletSynopsis rightSynopsis;
    private WaveletSynopsis joinedSynopsis;
    private WaveletSynopsis expectedSynopsis;
    private RandomDataGenerator rnd;

    private static int NUM_RECORDS = 50;

    public WaveletRandomJoinTest() {
        super(Integer.MAX_VALUE);
        rnd = new RandomDataGenerator();
    }

    @DataPoints
    public static List<DomainConstants> domains = Arrays.asList(Domain_Long, Domain_Integer, Domain_Short, Domain_Byte);

    private List<TransformTuple> generateRandomData(DomainConstants consts, int numRecords) {
        List<TransformTuple> result = new ArrayList<>();
        Set<Long> generatedPositions = new HashSet<>();
        for (int i = 0; i < numRecords; i++) {
            Long newPosition;
            do {
                newPosition = rnd.nextLong(consts.domainStart, consts.domainEnd);
            } while (generatedPositions.contains(newPosition));
            generatedPositions.add(newPosition);
            result.add(new TransformTuple(newPosition, rnd.nextLong(1, 10)));
        }
        return result;
    }

    private List<TransformTuple> generateCorrelatedData(DomainConstants consts, List<TransformTuple> inputData) {
        List<TransformTuple> result = generateRandomData(consts, NUM_RECORDS / 2);
        Set<Long> generatedPositions = result.stream().map(x -> x.position).collect(Collectors.toSet());
        while (result.size() < NUM_RECORDS) {
            TransformTuple t = inputData.get(RandomUtils.nextInt(0, inputData.size()));
            if (!generatedPositions.contains(t.position) && RandomUtils.nextBoolean()) {
                result.add(new TransformTuple(t.position, rnd.nextLong(1, 10)));
                generatedPositions.add(t.position);
            }
        }
        return result;
    }

    private List<TransformTuple> getJoinedData(List<TransformTuple> rData, List<TransformTuple> sData) {
        List<TransformTuple> joinedResult = new ArrayList<>();
        Map<Long, Long> rCardinality = new HashMap<>();
        Map<Long, Long> sCardinality = new HashMap<>();
        for (TransformTuple t : rData) {
            rCardinality.put(t.position, t.cardinality);
        }
        for (TransformTuple t : sData) {
            sCardinality.put(t.position, t.cardinality);
        }
        for (Map.Entry<Long, Long> rItem : rCardinality.entrySet()) {
            Long sItemCardinality = sCardinality.get(rItem.getKey());
            if (sItemCardinality != null) {
                joinedResult.add(new TransformTuple(rItem.getKey(), rItem.getValue() * sItemCardinality));
            }
        }
        return joinedResult;
    }

    private List<TransformTuple> leftData;
    private List<TransformTuple> rightData;
    private List<TransformTuple> joinedData;

    private void init(WaveletSynopsisSupplier waveletSupplier, DomainConstants consts, Boolean normalize)
            throws Exception {
        leftSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        rightSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        joinedSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        expectedSynopsis = waveletSupplier.createSynopsis(consts, threshold, normalize);
        AbstractIntegerSynopsisBuilder leftBuilder = waveletSupplier.createSynopsisBuilder(leftSynopsis);
        AbstractIntegerSynopsisBuilder rightBuilder = waveletSupplier.createSynopsisBuilder(rightSynopsis);
        AbstractIntegerSynopsisBuilder joinedBuilder = waveletSupplier.createSynopsisBuilder(expectedSynopsis);
        leftData = generateRandomData(consts, NUM_RECORDS);
        rightData = generateCorrelatedData(consts, leftData);
        joinedData = getJoinedData(leftData, rightData);
        TransformHelper.runTransform(leftData, leftBuilder);
        TransformHelper.runTransform(rightData, rightBuilder);
        TransformHelper.runTransform(joinedData, joinedBuilder);
    }

    @Theory
    @Repeat(times = 100)
    public void testJoinRelations(@FromDataPoints("rawWavelet") WaveletSynopsisSupplier waveletSupplier,
            DomainConstants consts, Boolean normalize) throws Exception {
        init(waveletSupplier, consts, normalize);
        joinedSynopsis.join(leftSynopsis, rightSynopsis);
        joinedSynopsis.createBinaryPreorder();
        expectedSynopsis.createBinaryPreorder();
        PeekingIterator<WaveletCoefficient> joinedIt = new PeekingIterator<>(joinedSynopsis.getElements().iterator());
        Iterator<WaveletCoefficient> compareIt = expectedSynopsis.getElements().iterator();
        while (compareIt.hasNext()) {
            WaveletCoefficient compareCoeff = compareIt.next();
            double joinedCoeffValue =
                    joinedSynopsis.findCoeffValue(joinedIt, compareCoeff.getIdx(), compareCoeff.getLevel());
            assertEquals(compareCoeff.getValue(), joinedCoeffValue,
                    compareCoeff.getValue() == 0.0 ? epsilon : Math.abs(compareCoeff.getValue() * epsilon));
        }
    }
}
