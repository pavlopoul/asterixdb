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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;

public class SynopsisElementFactory {

    public static ISynopsisElement createSynopsisElement(SynopsisType type, long key, double value, long uniqueValNum,
            ITypeTraits keyTypeTraits) throws HyracksDataException {
        switch (type) {
            case ContinuousHistogram:
                return new HistogramBucket(key, value);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis element of type " + type);
        }
    }

    public static Collection<? extends ISynopsisElement> createSynopsisElementsCollection(SynopsisType type,
            int elementsNum) throws HyracksDataException {
        Collection<? extends ISynopsisElement> elements;
        switch (type) {
            case QuantileSketch:
                elements = new ArrayList<>(elementsNum);
                break;
            default:
                throw new HyracksDataException("Cannot new elements collection for synopsis type " + type);
        }
        return elements;
    }
}
