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

import java.util.List;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.statistics.common.AbstractIntegerSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformHelper;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;

public abstract class WaveletTransformTest extends WaveletTest {

    protected AbstractIntegerSynopsisBuilder builder;
    protected WaveletSynopsis synopsis;

    public WaveletTransformTest(int threshold) {
        super(threshold);
    }

    protected void init(WaveletSynopsisSupplier synopsisSupplier, DomainConstants domainConsts, Boolean normalize)
            throws HyracksDataException {
        synopsis = synopsisSupplier.createSynopsis(domainConsts, threshold, normalize);
        builder = synopsisSupplier.createSynopsisBuilder(synopsis);
    }

    public PeekingIterator<WaveletCoefficient> runTest(List<TransformTuple> initialData) throws Exception {
        TransformHelper.runTransform(initialData, builder);
        return new PeekingIterator<>(synopsis.getElements().iterator());
    }
}
