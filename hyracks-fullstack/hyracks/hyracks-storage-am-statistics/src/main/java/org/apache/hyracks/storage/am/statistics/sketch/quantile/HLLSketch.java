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
package org.apache.hyracks.storage.am.statistics.sketch.quantile;

import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import net.agkn.hll.util.BitUtil;
import net.agkn.hll.util.BitVector;
import net.agkn.hll.util.HLLUtil;
import net.agkn.hll.util.LongIterator;
import net.agkn.hll.util.NumberUtil;

public class HLLSketch {
    public static final int MINIMUM_LOG2M_PARAM = 4;
    public static final int MAXIMUM_LOG2M_PARAM = 30;

    // minimum and maximum values for the register width of the HLL
    public static final int MINIMUM_REGWIDTH_PARAM = 1;
    public static final int MAXIMUM_REGWIDTH_PARAM = 8;
    //
    //    // minimum and maximum values for the 'expthresh' parameter of the
    //    // constructor that is meant to match the PostgreSQL implementation's
    //    // constructor and parameter names
    public static final int MINIMUM_EXPTHRESH_PARAM = -1;
    public static final int MAXIMUM_EXPTHRESH_PARAM = 18;
    public static final int MAXIMUM_EXPLICIT_THRESHOLD = (1 << (MAXIMUM_EXPTHRESH_PARAM - 1)/*per storage spec*/);
    //
    //    // ************************************************************************
    // Storage
    // storage used when #type is EXPLICIT, null otherwise
    private LongOpenHashSet explicitStorage;
    //    // storage used when #type is SPARSE, null otherwise
    private Int2ByteOpenHashMap sparseProbabilisticStorage;
    //    // storage used when #type is FULL, null otherwise
    private BitVector probabilisticStorage;
    //
    //    // current type of this HLL instance, if this changes then so should the
    //    // storage used (see above)
    private HLLType type;
    //
    //    // ------------------------------------------------------------------------
    //    // Characteristic parameters
    //    // NOTE:  These members are named to match the PostgreSQL implementation's
    //    //        parameters.
    //    // log2(the number of probabilistic HLL registers)
    private final int log2m;
    //    // the size (width) each register in bits
    private final int regwidth;
    //
    //    // ------------------------------------------------------------------------
    //    // Computed constants
    //    // ........................................................................
    //    // EXPLICIT-specific constants
    //    // flag indicating if the EXPLICIT representation should NOT be used
    private final boolean explicitOff;
    //    // flag indicating that the promotion threshold from EXPLICIT should be
    //    // computed automatically
    //    // NOTE:  this only has meaning when 'explicitOff' is false
    private final boolean explicitAuto;
    //    // threshold (in element count) at which a EXPLICIT HLL is converted to a
    //    // SPARSE or FULL HLL, always greater than or equal to zero and always a
    //    // power of two OR simply zero
    //    // NOTE:  this only has meaning when 'explicitOff' is false
    private final int explicitThreshold;
    //
    //    // ........................................................................
    //    // SPARSE-specific constants
    //    // the computed width of the short words
    private final int shortWordLength;
    //    // flag indicating if the SPARSE representation should not be used
    private final boolean sparseOff;
    //    // threshold (in register count) at which a SPARSE HLL is converted to a
    //    // FULL HLL, always greater than zero
    private final int sparseThreshold;
    //
    //    // ........................................................................
    //    // Probabilistic algorithm constants
    //    // the number of registers, will always be a power of 2
    private final int m;
    //    // a mask of the log2m bits set to one and the rest to zero
    private final int mBitsMask;
    //    // a mask as wide as a register (see #fromBytes())
    private final int valueMask;
    //    // mask used to ensure that p(w) does not overflow register (see #Constructor() and #addRaw())
    private final long pwMaxMask;
    //    // alpha * m^2 (the constant in the "'raw' HyperLogLog estimator")
    private final double alphaMSquared;
    //    // the cutoff value of the estimator for using the "small" range cardinality
    //    // correction formula
    private final double smallEstimatorCutoff;
    //    // the cutoff value of the estimator for using the "large" range cardinality
    //    // correction formula
    private final double largeEstimatorCutoff;

    public HLLSketch(final int log2m, final int regwidth, final int expthresh, final boolean sparseon,
            final HLLType type) {
        this.log2m = log2m;
        if ((log2m < MINIMUM_LOG2M_PARAM) || (log2m > MAXIMUM_LOG2M_PARAM)) {
            throw new IllegalArgumentException("'log2m' must be at least " + MINIMUM_LOG2M_PARAM + " and at most "
                    + MAXIMUM_LOG2M_PARAM + " (was: " + log2m + ")");
        }

        this.regwidth = regwidth;
        if ((regwidth < MINIMUM_REGWIDTH_PARAM) || (regwidth > MAXIMUM_REGWIDTH_PARAM)) {
            throw new IllegalArgumentException("'regwidth' must be at least " + MINIMUM_REGWIDTH_PARAM + " and at most "
                    + MAXIMUM_REGWIDTH_PARAM + " (was: " + regwidth + ")");
        }

        this.m = (1 << log2m);
        this.mBitsMask = m - 1;
        this.valueMask = (1 << regwidth) - 1;
        this.pwMaxMask = HLLUtil.pwMaxMask(regwidth);
        this.alphaMSquared = HLLUtil.alphaMSquared(m);
        this.smallEstimatorCutoff = HLLUtil.smallEstimatorCutoff(m);
        this.largeEstimatorCutoff = HLLUtil.largeEstimatorCutoff(log2m, regwidth);

        if (expthresh == -1) {
            this.explicitAuto = true;
            this.explicitOff = false;

            // NOTE:  This math matches the size calculation in the PostgreSQL impl.
            final long fullRepresentationSize =
                    (this.regwidth * (long) this.m + 7/*round up to next whole byte*/) / Byte.SIZE;
            final int numLongs = (int) (fullRepresentationSize / 8/*integer division to round down*/);

            if (numLongs > MAXIMUM_EXPLICIT_THRESHOLD) {
                this.explicitThreshold = MAXIMUM_EXPLICIT_THRESHOLD;
            } else {
                this.explicitThreshold = numLongs;
            }
        } else if (expthresh == 0) {
            this.explicitAuto = false;
            this.explicitOff = true;
            this.explicitThreshold = 0;
        } else if ((expthresh > 0) && (expthresh <= MAXIMUM_EXPTHRESH_PARAM)) {
            this.explicitAuto = false;
            this.explicitOff = false;
            this.explicitThreshold = (1 << (expthresh - 1));
        } else {
            throw new IllegalArgumentException("'expthresh' must be at least " + MINIMUM_EXPTHRESH_PARAM
                    + " and at most " + MAXIMUM_EXPTHRESH_PARAM + " (was: " + expthresh + ")");
        }

        this.shortWordLength = (regwidth + log2m);
        this.sparseOff = !sparseon;
        if (this.sparseOff) {
            this.sparseThreshold = 0;
        } else {
            // TODO improve this cutoff to include the cost overhead of Java
            //      members/objects
            final int largestPow2LessThanCutoff =
                    (int) NumberUtil.log2((this.m * this.regwidth) / this.shortWordLength);
            this.sparseThreshold = (1 << largestPow2LessThanCutoff);
        }

        initializeStorage(type);
    }

    /**
     * Construct an empty HLL with the given {@code log2m} and {@code regwidth}.
     * <p/>
     *
     * This is equivalent to calling <code>HLL(log2m, regwidth, -1, true, HLLType.EMPTY)</code>.
     *
     * @param log2m
     *            log-base-2 of the number of registers used in the HyperLogLog
     *            algorithm. Must be at least 4 and at most 30.
     * @param regwidth
     *            number of bits used per register in the HyperLogLog
     *            algorithm. Must be at least 1 and at most 8.
     *
     * @see #HLL(int, int, int, boolean, HLLType)
     */
    public HLLSketch(final int log2m, final int regwidth) {
        this(log2m, regwidth, -1, true, HLLType.EMPTY);
    }

    public HLLType getType() {
        return type;
    }

    // -------------------------------------------------------------------------

    public void addRaw(final long rawValue) {
        switch (type) {
            case EMPTY: {
                // NOTE:  EMPTY type is always promoted on #addRaw()
                if (this.explicitThreshold > 0) {
                    initializeStorage(HLLType.EXPLICIT);
                    explicitStorage.add(rawValue);
                } else if (!sparseOff) {
                    initializeStorage(HLLType.SPARSE);
                    addRawSparseProbabilistic(rawValue);
                } else {
                    initializeStorage(HLLType.FULL);
                    addRawProbabilistic(rawValue);
                }
                return;
            }
            case EXPLICIT: {
                explicitStorage.add(rawValue);

                // promotion, if necessary
                if (explicitStorage.size() > explicitThreshold) {
                    if (!sparseOff) {
                        initializeStorage(HLLType.SPARSE);
                        for (final long value : explicitStorage) {
                            addRawSparseProbabilistic(value);
                        }
                    } else {
                        initializeStorage(HLLType.FULL);
                        for (final long value : explicitStorage) {
                            addRawProbabilistic(value);
                        }
                    }
                    explicitStorage = null;
                }
                return;
            }
            case SPARSE: {
                addRawSparseProbabilistic(rawValue);

                // promotion, if necessary
                if (sparseProbabilisticStorage.size() > sparseThreshold) {
                    initializeStorage(HLLType.FULL);
                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }
                    sparseProbabilisticStorage = null;
                }
                return;
            }
            case FULL:
                addRawProbabilistic(rawValue);
                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    private void addRawSparseProbabilistic(final long rawValue) {
        // p(w): position of the least significant set bit (one-indexed)
        // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
        //
        // By construction of pwMaxMask (see #Constructor()),
        //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
        // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
        // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
        final long substreamValue = (rawValue >>> log2m);
        final byte p_w;

        if (substreamValue == 0L) {
            // The paper does not cover p(0x0), so the special value 0 is used.
            // 0 is the original initialization value of the registers, so by
            // doing this the multiset simply ignores it. This is acceptable
            // because the probability is 1/(2^(2^registerSizeInBits)).
            p_w = 0;
        } else {
            p_w = (byte) (1 + BitUtil.leastSignificantBit(substreamValue | pwMaxMask));
        }

        // Short-circuit if the register is being set to zero, since algorithmically
        // this corresponds to an "unset" register, and "unset" registers aren't
        // stored to save memory. (The very reason this sparse implementation
        // exists.) If a register is set to zero it will break the #algorithmCardinality
        // code.
        if (p_w == 0) {
            return;
        }

        // NOTE:  no +1 as in paper since 0-based indexing
        final int j = (int) (rawValue & mBitsMask);

        final byte currentValue = sparseProbabilisticStorage.get(j);
        if (p_w > currentValue) {
            sparseProbabilisticStorage.put(j, p_w);
        }
    }

    private void addRawProbabilistic(final long rawValue) {
        // p(w): position of the least significant set bit (one-indexed)
        // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
        //
        // By construction of pwMaxMask (see #Constructor()),
        //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
        // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
        // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
        final long substreamValue = (rawValue >>> log2m);
        final byte p_w;

        if (substreamValue == 0L) {
            // The paper does not cover p(0x0), so the special value 0 is used.
            // 0 is the original initialization value of the registers, so by
            // doing this the multiset simply ignores it. This is acceptable
            // because the probability is 1/(2^(2^registerSizeInBits)).
            p_w = 0;
        } else {
            p_w = (byte) (1 + BitUtil.leastSignificantBit(substreamValue | pwMaxMask));
        }

        // Short-circuit if the register is being set to zero, since algorithmically
        // this corresponds to an "unset" register, and "unset" registers aren't
        // stored to save memory. (The very reason this sparse implementation
        // exists.) If a register is set to zero it will break the #algorithmCardinality
        // code.
        if (p_w == 0) {
            return;
        }

        // NOTE:  no +1 as in paper since 0-based indexing
        final int j = (int) (rawValue & mBitsMask);

        probabilisticStorage.setMaxRegister(j, p_w);
    }

    private void initializeStorage(final HLLType type) {
        this.type = type;
        switch (type) {
            case EMPTY:
                // nothing to be done
                break;
            case EXPLICIT:
                this.explicitStorage = new LongOpenHashSet();
                break;
            case SPARSE:
                this.sparseProbabilisticStorage = new Int2ByteOpenHashMap();
                break;
            case FULL:
                this.probabilisticStorage = new BitVector(regwidth, m);
                break;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    public long cardinality() {
        switch (type) {
            case EMPTY:
                return 0/*by definition*/;
            case EXPLICIT:
                return explicitStorage.size();
            case SPARSE:
                return (long) Math.ceil(sparseProbabilisticAlgorithmCardinality());
            case FULL:
                return (long) Math.ceil(fullProbabilisticAlgorithmCardinality());
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    double sparseProbabilisticAlgorithmCardinality() {
        final int m = this.m/*for performance*/;

        // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
        // 'j'th register value
        double sum = 0;
        int numberOfZeroes = 0/*"V" in the paper*/;
        for (int j = 0; j < m; j++) {
            final long register = sparseProbabilisticStorage.get(j);

            sum += 1.0 / (1L << register);
            if (register == 0L)
                numberOfZeroes++;
        }

        // apply the estimate and correction to the indicator function
        final double estimator = alphaMSquared / sum;
        if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
            return HLLUtil.smallEstimator(m, numberOfZeroes);
        } else if (estimator <= largeEstimatorCutoff) {
            return estimator;
        } else {
            return HLLUtil.largeEstimator(log2m, regwidth, estimator);
        }
    }

    double fullProbabilisticAlgorithmCardinality() {
        final int m = this.m/*for performance*/;

        // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
        // 'j'th register value
        double sum = 0;
        int numberOfZeroes = 0/*"V" in the paper*/;
        final LongIterator iterator = probabilisticStorage.registerIterator();
        while (iterator.hasNext()) {
            final long register = iterator.next();

            sum += 1.0 / (1L << register);
            if (register == 0L)
                numberOfZeroes++;
        }

        // apply the estimate and correction to the indicator function
        final double estimator = alphaMSquared / sum;
        if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff)) {
            return HLLUtil.smallEstimator(m, numberOfZeroes);
        } else if (estimator <= largeEstimatorCutoff) {
            return estimator;
        } else {
            return HLLUtil.largeEstimator(log2m, regwidth, estimator);
        }
    }

    public void clear() {
        switch (type) {
            case EMPTY:
                return /*do nothing*/;
            case EXPLICIT:
                explicitStorage.clear();
                return;
            case SPARSE:
                sparseProbabilisticStorage.clear();
                return;
            case FULL:
                probabilisticStorage.fill(0);
                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    public void union(final HLLSketch other) {
        // TODO: verify HLLs are compatible
        final HLLType otherType = other.getType();

        if (type.equals(otherType)) {
            homogeneousUnion(other);
            return;
        } else {
            heterogenousUnion(other);
            return;
        }
    }

            // ------------------------------------------------------------------------
            // Union helpers
            /**
             * Computes the union of two HLLs, of different types, and stores the
             * result in this instance.
             *
             * @param other
             *            the other {@link HLL} instance to union into this one. This
             *            cannot be <code>null</code>.
             */
            /*package, for testing*/ void heterogenousUnion(final HLLSketch other) {
        /*
         * The logic here is divided into two sections: unions with an EMPTY
         * HLL, and unions between EXPLICIT/SPARSE/FULL
         * HLL.
         *
         * Between those two sections, all possible heterogeneous unions are
         * covered. Should another type be added to HLLType whose unions
         * are not easily reduced (say, as EMPTY's are below) this may be more
         * easily implemented as Strategies. However, that is unnecessary as it
         * stands.
         */

        // ....................................................................
        // Union with an EMPTY
        if (HLLType.EMPTY.equals(type)) {
            // NOTE:  The union of empty with non-empty HLL is just a
            //        clone of the non-empty.

            switch (other.getType()) {
                case EXPLICIT: {
                    // src:  EXPLICIT
                    // dest: EMPTY

                    if (other.explicitStorage.size() <= explicitThreshold) {
                        type = HLLType.EXPLICIT;
                        explicitStorage = other.explicitStorage.clone();
                    } else {
                        if (!sparseOff) {
                            initializeStorage(HLLType.SPARSE);
                        } else {
                            initializeStorage(HLLType.FULL);
                        }
                        for (final long value : other.explicitStorage) {
                            addRaw(value);
                        }
                    }
                    return;
                }
                case SPARSE: {
                    // src:  SPARSE
                    // dest: EMPTY

                    if (!sparseOff) {
                        type = HLLType.SPARSE;
                        sparseProbabilisticStorage = other.sparseProbabilisticStorage.clone();
                    } else {
                        initializeStorage(HLLType.FULL);
                        for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                            final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                            probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                        }
                    }
                    return;
                }
                default/*case FULL*/: {
                    // src:  FULL
                    // dest: EMPTY

                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();
                    return;
                }
            }
        } else if (HLLType.EMPTY.equals(other.getType())) {
            // source is empty, so just return destination since it is unchanged
            return;
        } /* else -- both of the sets are not empty */

        // ....................................................................
        // NOTE: Since EMPTY is handled above, the HLLs are non-EMPTY below
        switch (type) {
            case EXPLICIT: {
                // src:  FULL/SPARSE
                // dest: EXPLICIT
                // "Storing into destination" cannot be done (since destination
                // is by definition of smaller capacity than source), so a clone
                // of source is made and values from destination are inserted
                // into that.

                // Determine source and destination storage.
                // NOTE:  destination storage may change through promotion if
                //        source is SPARSE.
                if (HLLType.SPARSE.equals(other.getType())) {
                    if (!sparseOff) {
                        type = HLLType.SPARSE;
                        sparseProbabilisticStorage = other.sparseProbabilisticStorage.clone();
                    } else {
                        initializeStorage(HLLType.FULL);
                        for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                            final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                            probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                        }
                    }
                } else /*source is HLLType.FULL*/ {
                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();
                }
                for (final long value : explicitStorage) {
                    addRaw(value);
                }
                explicitStorage = null;
                return;
            }
            case SPARSE: {
                if (HLLType.EXPLICIT.equals(other.getType())) {
                    // src:  EXPLICIT
                    // dest: SPARSE
                    // Add the raw values from the source to the destination.

                    for (final long value : other.explicitStorage) {
                        addRaw(value);
                    }
                    // NOTE:  addRaw will handle promotion cleanup
                } else /*source is HLLType.FULL*/ {
                    // src:  FULL
                    // dest: SPARSE
                    // "Storing into destination" cannot be done (since destination
                    // is by definition of smaller capacity than source), so a
                    // clone of source is made and registers from the destination
                    // are merged into the clone.

                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();
                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }
                    sparseProbabilisticStorage = null;
                }
                return;
            }
            default/*destination is HLLType.FULL*/: {
                if (HLLType.EXPLICIT.equals(other.getType())) {
                    // src:  EXPLICIT
                    // dest: FULL
                    // Add the raw values from the source to the destination.
                    // Promotion is not possible, so don't bother checking.

                    for (final long value : other.explicitStorage) {
                        addRaw(value);
                    }
                } else /*source is HLLType.SPARSE*/ {
                    // src:  SPARSE
                    // dest: FULL
                    // Merge the registers from the source into the destination.
                    // Promotion is not possible, so don't bother checking.

                    for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }
                }

            }
        }
    }

    /**
     * Computes the union of two HLLs of the same type, and stores the
     * result in this instance.
     *
     * @param other
     *            the other {@link HLL} instance to union into this one. This
     *            cannot be <code>null</code>.
     */
    private void homogeneousUnion(final HLLSketch other) {
        switch (type) {
            case EMPTY:
                // union of empty and empty is empty
                return;
            case EXPLICIT:
                for (final long value : other.explicitStorage) {
                    addRaw(value);
                }
                // NOTE:  #addRaw() will handle promotion, if necessary
                return;
            case SPARSE:
                for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                    final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                    final byte currentRegisterValue = sparseProbabilisticStorage.get(registerIndex);
                    if (registerValue > currentRegisterValue) {
                        sparseProbabilisticStorage.put(registerIndex, registerValue);
                    }
                }

                // promotion, if necessary
                if (sparseProbabilisticStorage.size() > sparseThreshold) {
                    initializeStorage(HLLType.FULL);
                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }
                    sparseProbabilisticStorage = null;
                }
                return;
            case FULL:
                for (int i = 0; i < m; i++) {
                    final long registerValue = other.probabilisticStorage.getRegister(i);
                    probabilisticStorage.setMaxRegister(i, registerValue);
                }
                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    public LongOpenHashSet getExplicit() {
        return explicitStorage;
    }

    public long[] getWords() {
        return probabilisticStorage.words();
    }

    public Int2ByteOpenHashMap getSparse() {
        return sparseProbabilisticStorage;
    }

}
