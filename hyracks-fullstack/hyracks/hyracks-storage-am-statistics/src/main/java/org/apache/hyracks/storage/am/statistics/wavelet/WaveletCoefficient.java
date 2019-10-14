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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public class WaveletCoefficient implements ISynopsisElement<Long> {

    private static final long serialVersionUID = 1L;

    private double value = 0.0;
    private int level = -1;
    private long index = -1L;

    public static Comparator<WaveletCoefficient> KEY_COMPARATOR = new KeyComparator();

    private static class KeyComparator implements Comparator<WaveletCoefficient>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(WaveletCoefficient o1, WaveletCoefficient o2) {
            return Long.compareUnsigned(o1.getIdx(), o2.getIdx());
        }
    }

    public static Comparator<WaveletCoefficient> VALUE_COMPARATOR = new ValueComparator();

    private static class ValueComparator implements Comparator<WaveletCoefficient>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        // default comparator based on absolute coefficient value
        public int compare(WaveletCoefficient o1, WaveletCoefficient o2) {
            return Double.compare(Math.abs(o1.getValue()), Math.abs(o2.getValue()));
        }
    };

    public WaveletCoefficient(double value, int level, long index) {
        reset(value, level, index);
    }

    public void reset(WaveletCoefficient c) {
        this.value = c.getValue();
        this.level = c.getLevel();
        this.index = c.getIdx();
    }

    public void reset(double value, int level, long index) {
        this.value = value;
        this.level = level;
        this.index = index;
    }

    @Override
    public Long getKey() {
        return index;
    }

    public long getIdx() {
        return index;
    }

    @Override
    public double getValue() {
        return value;
    }

    public int getLevel() {
        return level;
    }

    public Double setValue(Double value) {
        Double oldValue = this.value;
        this.value = value;
        return oldValue;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaveletCoefficient)) {
            return false;
        }
        WaveletCoefficient coeff = (WaveletCoefficient) o;
        return (coeff.value - value) < DoublePointable.getEpsilon() && (coeff.level == level) && (coeff.index == index);
    }

    @Override
    public String toString() {
        return "L" + level + ": h" + index + "=" + value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, level, index);
    }

    public boolean isDummy() {
        return level < 0;
    }

    public static double getNormalizationCoefficient(int maxLevel, int level) {
        return (1 << ((maxLevel - level) / 2)) * ((((maxLevel - level) % 2) == 0) ? 1 : Math.sqrt(2));
    }

    // Returns index of the parent coefficient
    public long getParentCoeffIndex(long domainMin, int maxLevel) {
        return getAncestorCoeffIndex(domainMin, maxLevel, 1);
    }

    public long getAncestorCoeffIndex(long domainMin, int maxLevel, int ancestorLevelDifference) {
        if (ancestorLevelDifference == 0) {
            return index;
        }
        // Special case for values on level 0
        if (level == 0) {
            // Convert position to proper coefficient index
            return (index - domainMin) >>> ancestorLevelDifference | (1l << (maxLevel - ancestorLevelDifference));
        } else if (level == maxLevel + 1) {
            // Special case for the main average with level maxLevel+1
            return -1;
        } else if (ancestorLevelDifference >= Long.SIZE) {
            return 0L;
        } else {
            return index >>> ancestorLevelDifference;
        }
    }

    public long getLeftChildCoeffIndex(long domainMin, int maxLevel) {
        if (level == 0) {
            return -1;
        } else if (level > maxLevel) {
            return 1;
        } else if (level == 1) {
            return ((index - (1L << (maxLevel - 1))) << 1) + domainMin;
        } else {
            return index << 1;
        }
    }

    public long getRightChildCoeffIndex(long domainMin, int maxLevel) {
        if (level == 0) {
            return -1;
        } else if (level > maxLevel) {
            return 1;
        } else {
            return getLeftChildCoeffIndex(domainMin, maxLevel) + 1;
        }
    }

    // Returns true if the coefficient's support interval covers the given position
    public boolean covers(long position, int maxLevel, long domainMin) {
        if (level < 0) {
            return true;
        } else if (level == 0) {
            return index == position;
        } else {
            return index == (((position - domainMin) >>> 1) + (1l << (maxLevel - 1))) >>> (level - 1);
        }
    }

    public DyadicTupleRange convertCoeffToSupportInterval(long domainMin, long domainMax, int maxLevel) {
        if (level == maxLevel) {
            return new DyadicTupleRange(domainMin, domainMax, 0.0);
        } else {
            long intervalStart = convertCoeffToPosition(domainMin, domainMax);
            return new DyadicTupleRange(intervalStart, intervalStart + (1L << level) - 1, 0.0);
        }
    }

    // Method calculates coefficient level based on it's index and maximum level of transform. Since coefficient level
    // is a transient information the method is used to initialize coefficient with appropriate level value.
    public static int getLevel(long coeffIdx, int maxLevel) {
        if (coeffIdx == 0) {
            return maxLevel + 1;
        }
        int level = 0;
        coeffIdx = coeffIdx >>> 1;
        while (coeffIdx > 0) {
            coeffIdx = coeffIdx >>> 1;
            level++;
        }
        return maxLevel - level;
    }

    public long convertCoeffToPosition(long domainMin, long domainMax) {
        // Position is calculated via formula coeff << (maxLevel - level) - domainLength + domainStart.
        // Domain length is expanded like domainEnd-domainStart, to avoid integer overflow
        //return (coeff << level) - domainEnd + domainStart + domainStart;

        if (level == 0)
            return index;
        else {
            //binary mask, used to zero out most significant bit
            long mask = domainMax - 1 - domainMin;
            return ((index << level) & mask) + domainMin;
        }
    }
}