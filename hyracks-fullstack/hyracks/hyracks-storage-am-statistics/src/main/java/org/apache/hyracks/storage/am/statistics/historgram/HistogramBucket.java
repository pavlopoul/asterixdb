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
package org.apache.hyracks.storage.am.statistics.historgram;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public class HistogramBucket implements ISynopsisElement<Long> {

    private static final long serialVersionUID = 1L;

    private long rightBorder;
    private double value;

    public HistogramBucket(long border, double value) {
        this.rightBorder = border;
        this.value = value;
    }

    public void setRightBorder(long rightBorder) {
        this.rightBorder = rightBorder;
    }

    public void appendToValue(double appendedValue) {
        this.value += appendedValue;
    }

    @Override
    public Long getKey() {
        return rightBorder;
    }

    @Override
    public double getValue() {
        return value;
    }
}
