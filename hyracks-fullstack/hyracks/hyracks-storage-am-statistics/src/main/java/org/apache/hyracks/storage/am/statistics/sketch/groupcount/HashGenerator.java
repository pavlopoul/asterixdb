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

public class HashGenerator {

    private static int MOD = 2147483647;
    private static int HL = 31;

    public static int pairwiseIndependent(long a[], long x) {
        return pairwiseIndependent(a[0], a[1], x);
    }

    public static int pairwiseIndependent(long a, long b, long x) {

        long result;

        // return a hash of x using a and b mod (2^31 - 1)
        // may need to do another mod afterwards, or drop high bits
        // depending on d, number of bad guys
        // 2^31 - 1 = 2147483647

        //  result = ((long long) a)*((long long) x)+((long long) b);
        result = (a * x) + b;
        result = ((result >> HL) + result) & MOD;

        return (int) result;
    }

    /**
     * Method generates a vector of products [ x^3, x^2, x ] of the parameter x
     * 
     * @param x
     * @return
     */
    public static long[] productVector(long x) {
        return new long[] { x * x * x, x * x, x };
    }

    public static int fourwiseIndependent(long[] coeffs, long[] products) {
        long result;

        // returns values that are 4-wise independent calculated as a*x^3 + b*x^2+ c*x + d
        result = coeffs[0] * products[0] + coeffs[1] * products[1] + coeffs[2] * products[2] + coeffs[3];
        result = ((result >> HL) + result) & MOD;
        return (int) result;
    }
}
