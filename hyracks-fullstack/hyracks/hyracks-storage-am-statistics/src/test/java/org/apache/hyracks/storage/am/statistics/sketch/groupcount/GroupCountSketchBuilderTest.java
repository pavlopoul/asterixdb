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

import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.junit.Before;

public class GroupCountSketchBuilderTest {
    protected GroupCountSketchBuilder builder;
    protected WaveletSynopsis synopsis;

    private final long domainStart;
    private final long domainEnd;
    private final int maxLevel;
    private final int synopsisSize;
    private final int inputSize;
    private final boolean normalize;
    private final int fanout;

    protected final static double ACCURACY = 0.2;
    protected final static double ENERGY_ACCURACY = 0.01;
    protected final static int FAILURE_TRIES = 500;
    protected final static int FAILURE_NUM = 5;
    protected final static double FAILURE_PROBABILITY = (double) FAILURE_NUM / FAILURE_TRIES;

    public GroupCountSketchBuilderTest(long domainStart, long domainEnd, int maxLevel, int size, boolean normalize,
            int fanout, int inputSize) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
        this.maxLevel = maxLevel;
        this.synopsisSize = size;
        this.normalize = normalize;
        this.fanout = fanout;
        this.inputSize = inputSize;
    }

    @Before
    public void init() throws HyracksDataException {
        synopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, synopsisSize,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        builder = new GroupCountSketchBuilder(synopsis, "", "", "", "", false, null, new ComponentStatistics(-1L, -1L),
                fanout, FAILURE_PROBABILITY, ACCURACY, ENERGY_ACCURACY, inputSize, System.currentTimeMillis());
    }
}
