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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;
import java.util.Collection;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISynopsis<T extends ISynopsisElement> extends Serializable {

    enum SynopsisType {
        None(false, false, false),
        GroupCountSketch(true, false, false),
        PrefixSumWavelet(true, false, true),
        Wavelet(true, true, true),
        EquiWidthHistogram(true, true, true),
        UniformHistogram(false, false, true),
        ContinuousHistogram(false, false, true),
        QuantileSketch(true, false, false);

        private boolean isMergeable;
        private final boolean isJoinable;
        private boolean needsSortedOrder;

        SynopsisType(boolean isMergeable, boolean isJoinable, boolean needsSortedOrder) {
            this.isMergeable = isMergeable;
            this.isJoinable = isJoinable;
            this.needsSortedOrder = needsSortedOrder;
        }

        public boolean isMergeable() {
            return isMergeable;
        }

        public boolean isJoinable() {
            return isJoinable;
        }

        public boolean needsSortedOrder() {
            return needsSortedOrder;
        }
    }

    SynopsisType getType();

    int getSize();

    Collection<T> getElements();

    void merge(ISynopsis<T> mergeSynopsis) throws HyracksDataException;

    double pointQuery(long position);

    double rangeQuery(long startPosition, long endPosition);

}