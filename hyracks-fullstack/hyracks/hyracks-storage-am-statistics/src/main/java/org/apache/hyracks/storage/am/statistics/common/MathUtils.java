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

public class MathUtils {

    public static long safeAverage(long a, long b) {
        long aDividedBy2 = a >> 1;
        long bDividedBy2 = b >> 1;
        return Math.addExact(aDividedBy2, bDividedBy2);
    }

    public static double safeRangeMultiply(long rangeStart, long rangeEnd, double d) {
        double result = 0;
        long pivot = 0;
        if (rangeEnd < pivot) {
            result += (Math.min(pivot, rangeStart) - (rangeEnd + 1)) * d;
            result += d;
        }
        if (rangeStart > pivot) {
            result += (rangeStart - Math.max(pivot, rangeEnd)) * d;
        }
        result += d;
        return result;
    }
}
