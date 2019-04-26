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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.io.Serializable;

public class ComponentStatisticsId implements Serializable {

    private Long minValue;

    private Long maxValue;

    public ComponentStatisticsId(Long minValue, Long maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public Long getMinTimestamp() {
        return minValue;
    }

    public Long getMaxTimestamp() {
        return maxValue;
    }

    @Override
    public String toString() {
        return "[ " + String.valueOf(minValue) + ", " + String.valueOf(maxValue) + " ]";

    }

    @Override
    public int hashCode() {
        return 31 * minValue.hashCode() + maxValue.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ComponentStatisticsId)) {
            return false;
        }
        ComponentStatisticsId other = (ComponentStatisticsId) obj;
        if (!maxValue.equals(other.maxValue)) {
            return false;
        }
        if (!minValue.equals(other.minValue)) {
            return false;
        }
        return true;
    }
}
