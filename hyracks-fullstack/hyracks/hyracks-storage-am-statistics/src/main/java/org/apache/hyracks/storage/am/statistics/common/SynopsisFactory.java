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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.ContinuousHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.EquiWidthHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.UniformHistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.UniformHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.PrefixSumWaveletSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;

public class SynopsisFactory {
    @SuppressWarnings("unchecked")
    public static AbstractSynopsis<? extends ISynopsisElement<Long>> createSynopsis(SynopsisType type,
            ITypeTraits keyTypeTraits, Collection<? extends ISynopsisElement> synopsisElements,
            long synopsisElementsNum, int synopsisSize) throws HyracksDataException {
        long domainStart = TypeTraitsDomainUtils.minDomainValue(keyTypeTraits);
        long domainEnd = TypeTraitsDomainUtils.maxDomainValue(keyTypeTraits);
        int maxLevel = TypeTraitsDomainUtils.maxLevel(keyTypeTraits);
        switch (type) {
            case UniformHistogram:
                return new UniformHistogramSynopsis(domainStart, domainEnd, maxLevel, synopsisElementsNum, synopsisSize,
                        (List<UniformHistogramBucket>) synopsisElements);
            case ContinuousHistogram:
            case QuantileSketch:
                return new ContinuousHistogramSynopsis(domainStart, domainEnd, maxLevel, synopsisElementsNum,
                        synopsisSize, (List<HistogramBucket>) synopsisElements);
            case EquiWidthHistogram:
                return new EquiWidthHistogramSynopsis(domainStart, domainEnd, maxLevel, synopsisSize,
                        (List<HistogramBucket>) synopsisElements);
            case Wavelet:
            case GroupCountSketch:
                return new WaveletSynopsis(domainStart, domainEnd, maxLevel, synopsisSize,
                        (Collection<WaveletCoefficient>) synopsisElements, true, true);
            case PrefixSumWavelet:
                return new PrefixSumWaveletSynopsis(domainStart, domainEnd, maxLevel, synopsisSize,
                        (Collection<WaveletCoefficient>) synopsisElements, true, true);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis of type " + type);
        }
    }

    public static Collection<? extends ISynopsisElement> createSynopsisElements(SynopsisType type, int elementsNum)
            throws HyracksDataException {
        Collection<? extends ISynopsisElement> elements;
        switch (type) {
            case UniformHistogram:
            case ContinuousHistogram:
            case EquiWidthHistogram:
                elements = new ArrayList<>(elementsNum);
                break;
            case Wavelet:
            case PrefixSumWavelet:
                elements = new PriorityQueue<>(elementsNum, WaveletCoefficient.VALUE_COMPARATOR);
                break;
            default:
                throw new HyracksDataException("Cannot new elements for synopsis of type " + type);
        }
        return elements;
    }
}
