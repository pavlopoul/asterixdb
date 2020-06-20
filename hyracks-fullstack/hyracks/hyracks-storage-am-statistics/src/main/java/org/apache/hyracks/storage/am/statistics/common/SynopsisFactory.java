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
package org.apache.hyracks.storage.am.statistics.common;

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.ContinuousHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;

public class SynopsisFactory {
    @SuppressWarnings("unchecked")
    public static AbstractSynopsis<? extends ISynopsisElement<Long>> createSynopsis(SynopsisType type,
            ITypeTraits keyTypeTraits, Collection<? extends ISynopsisElement> synopsisElements,
            long synopsisElementsNum, int synopsisSize) throws HyracksDataException {
        long domainStart = TypeTraitsDomainUtils.minDomainValue(keyTypeTraits);
        long domainEnd = TypeTraitsDomainUtils.maxDomainValue(keyTypeTraits);
        int maxLevel = TypeTraitsDomainUtils.maxLevel(keyTypeTraits);
        switch (type) {
            case ContinuousHistogram:
            case QuantileSketch:
                return new ContinuousHistogramSynopsis(domainStart, domainEnd, maxLevel, synopsisElementsNum,
                        synopsisSize, (List<HistogramBucket>) synopsisElements);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis of type " + type);
        }
    }
}
