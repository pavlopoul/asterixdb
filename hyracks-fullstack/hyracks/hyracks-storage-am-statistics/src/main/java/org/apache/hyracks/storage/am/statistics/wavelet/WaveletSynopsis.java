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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.collections4.iterators.ReverseListIterator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;
import org.apache.hyracks.storage.am.statistics.common.MathUtils;

public class WaveletSynopsis extends AbstractSynopsis<WaveletCoefficient> {

    private static final long serialVersionUID = 1L;

    // Trigger wavelet coefficients to be normalized
    protected boolean normalize;
    // Trigger linear approximation of the prefix sum returned by synopsis
    protected boolean linearApproximation;

    public WaveletSynopsis(long domainStart, long domainEnd, int maxLevel, int size,
            Collection<WaveletCoefficient> coefficients, boolean normalize, boolean linearApproximation) {
        super(domainStart, domainEnd, maxLevel, size, coefficients);
        this.normalize = normalize;
        this.linearApproximation = linearApproximation;
    }

    public boolean isNormalized() {
        return normalize;
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.Wavelet;
    }

    private List<WaveletCoefficient> sortOnKeys() {
        List<WaveletCoefficient> sortedCoefficients = new ArrayList<>(synopsisElements);
        Collections.sort(sortedCoefficients, WaveletCoefficient.KEY_COMPARATOR);
        return sortedCoefficients;
    }

    // Adds a new coefficient to the transform, subject to thresholding
    public void addElement(WaveletCoefficient coeff, boolean normalize) {
        addElement(coeff.getIdx(), coeff.getValue(), coeff.getLevel(), normalize);
    }

    // Adds a new detail coefficient to the transform, subject to thresholding
    public void addElement(long index, double value, int level, boolean normalize) {
        WaveletCoefficient detailCoeff;
        if (normalize)
            value /= WaveletCoefficient.getNormalizationCoefficient(maxLevel, level);
        if (synopsisElements.size() < size) {
            detailCoeff = new WaveletCoefficient(value, level, index);
        } else {
            detailCoeff = ((Queue<WaveletCoefficient>) synopsisElements).poll();
            if (Math.abs(value) > Math.abs(detailCoeff.getValue())) {
                detailCoeff.setValue(value);
                detailCoeff.setIndex(index);
                detailCoeff.setLevel(level);
            }
        }
        synopsisElements.add(detailCoeff);
    }

    class TreeNode {
        private TreeNode left;
        private TreeNode right;
        private WaveletCoefficient value;

        public TreeNode(WaveletCoefficient value) {
            this.value = value;
        }

        public TreeNode getLeft() {
            return left;
        }

        public TreeNode getRight() {
            return right;
        }

        public WaveletCoefficient getValue() {
            return value;
        }
    }

    public void orderByKeys() {
        synopsisElements = sortOnKeys();
    }

    //function sorts wavelet coefficients in binary tree preorder
    public void createBinaryPreorder() {
        // sorting coefficients according their indices first
        List<WaveletCoefficient> sortedCoefficients = sortOnKeys();
        synopsisElements = new ArrayList<>(synopsisElements.size());
        //scan through the queue to find main average
        TreeNode transformTreeRoot = null;
        boolean fakeRoot = false;
        for (WaveletCoefficient w : sortedCoefficients) {
            if (w.getIdx() == 0) {
                synopsisElements.add(w);
                continue;
            } else if (w.getIdx() == 1) {
                transformTreeRoot = new TreeNode(w);
                continue;
            } else if (transformTreeRoot == null) {
                //create fake main average for the sake of correctness of the algorithm
                transformTreeRoot = new TreeNode(new WaveletCoefficient(0.0, maxLevel, 1));
                fakeRoot = true;
            }
            TreeNode insertedNode = new TreeNode(w);
            //traverse the tree root to leaf finding a place to insert the new node
            TreeNode activeNode = null;
            TreeNode nextNode = transformTreeRoot;
            boolean isLeft = false;
            while (nextNode != null) {
                if (nextNode.value.getLevel() < insertedNode.value.getLevel()) {
                    if (activeNode != null) {
                        if (isLeft) {
                            activeNode.left = insertedNode;
                        } else {
                            activeNode.right = insertedNode;
                        }
                    }
                    WaveletSynopsis.TreeNode tmp = nextNode;
                    nextNode = insertedNode;
                    insertedNode = tmp;
                } else {
                    //determine is it left or right child
                    //isLeft = (coeff.getKey() >> (coeff.getLevel() - nextNode.value.getLevel()) & 0x1) == 0;
                    activeNode = nextNode;
                    //determine is it left or right child
                    isLeft = (insertedNode.value
                            .getIdx() >> (Math.abs(insertedNode.value.getLevel() - nextNode.value.getLevel()) - 1)
                            & 0x1) == 0;
                    if (isLeft) {
                        nextNode = nextNode.left;
                    } else {
                        nextNode = nextNode.right;
                    }
                }
            }
            if (isLeft)
                activeNode.left = insertedNode;
            else
                activeNode.right = insertedNode;
        }

        preOrderTraversal(transformTreeRoot, transformTreeRoot, fakeRoot);
    }

    private void preOrderTraversal(TreeNode treeRoot, TreeNode node, boolean fakeRoot) {
        if (node != null && treeRoot != null) {
            if (!node.equals(treeRoot) || !fakeRoot)
                synopsisElements.add(node.getValue());
            if (node.getLeft() != null)
                preOrderTraversal(treeRoot, node.getLeft(), fakeRoot);
            if (node.getRight() != null)
                preOrderTraversal(treeRoot, node.getRight(), fakeRoot);
        }
    }

    @Override
    public void merge(List<ISynopsis<WaveletCoefficient>> synopsisList) throws HyracksDataException {
        super.merge(synopsisList);
        createBinaryPreorder();
    }

    @Override
    // Method implements naive synopsis merge, which just picks largest coefficients from the synopsis sum
    public void merge(ISynopsis<WaveletCoefficient> mergedSynopsis) throws HyracksDataException {
        if (mergedSynopsis.getType() != getType()) {
            return;
        }
        // sort synopsis coefficients based on keys
        Iterator<WaveletCoefficient> mergedIt = ((WaveletSynopsis) mergedSynopsis).sortOnKeys().iterator();
        Iterator<WaveletCoefficient> it = sortOnKeys().iterator();
        synopsisElements.clear();
        synopsisElements = new PriorityQueue<>(size, WaveletCoefficient.VALUE_COMPARATOR);
        WaveletCoefficient mergedEntry = null;
        WaveletCoefficient entry = null;
        if (mergedIt.hasNext()) {
            mergedEntry = mergedIt.next();
        }
        if (it.hasNext()) {
            entry = it.next();
        }
        while (mergedEntry != null || entry != null) {
            if ((mergedEntry != null && entry == null)
                    || (mergedEntry != null && entry != null && mergedEntry.getIdx() < entry.getIdx())) {
                addElement(mergedEntry.getIdx(), mergedEntry.getValue(), mergedEntry.getLevel(), false);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
            } else if ((entry != null && mergedEntry == null)
                    || (mergedEntry != null && entry != null && entry.getIdx() < mergedEntry.getIdx())) {
                addElement(entry.getIdx(), entry.getValue(), entry.getLevel(), false);
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            } else {
                addElement(entry.getIdx(), entry.getValue() + mergedEntry.getValue(), entry.getLevel(), false);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            }
        }
    }

    protected double findCoeffValue(PeekingIterator<WaveletCoefficient> it, long coeffIdx, int coeffLevel) {
        WaveletCoefficient curr = it.peek();
        if (curr == null)
            return 0.0;
        long currIdx;
        if (curr.getLevel() <= coeffLevel)
            currIdx = curr.getIdx() >> (coeffLevel - curr.getLevel());
        else
            currIdx = curr.getIdx() << (curr.getLevel() - coeffLevel);
        try {
            while (it.hasNext() && currIdx < coeffIdx) {
                it.next();
                curr = it.peek();
                if (curr == null)
                    return 0.0;
                if (curr.getLevel() <= coeffLevel)
                    currIdx = curr.getIdx() >> (coeffLevel - curr.getLevel());
                else
                    currIdx = curr.getIdx() << (curr.getLevel() - coeffLevel);
            }
        } catch (NoSuchElementException e) {
            return 0.0;
        }

        if (curr != null && currIdx == coeffIdx) {
            it.next();
            return curr.getValue();
        } else {
            return 0.0;
        }
    }

    //TODO:this is a duplicate of method WaveletCoefficient.getParentCoeffIndex()
    public long convertPositionToCoeffIndex(long position) {
        return ((position - domainStart) >>> 1) | (1L << (maxLevel - 1));
        //return (1l << (maxLevel - 1)) + ((position >> 1) - (domainStart >> 1));
    }

    @Override
    public double pointQuery(long position) {
        // point query is a special case of range query with range [x; x]
        return rangeQuery(position, position);
    }

    //    public Double rangeQuery2(Long startPosition, Long endPosition) {
    //        Double value = 0.0;
    //        PeekingIterator<? extends ISynopsisElement> it = new PeekingIterator<>(coefficients.iterator());
    //        Double mainAvg = findCoeffValue(it, 0l);
    //        List<DyadicTupleRange> workingSet = new ArrayList<>();
    //        Long convertedStartPos = convertPositionToCoeff(startPosition) << 1;
    //        Long convertedEndPos = convertPositionToCoeff(endPosition) << 1;
    //        workingSet.add(new DyadicTupleRange(startPosition - domainStart, endPosition - domainStart, mainAvg));
    //        int level = maxLevel;
    //        while (!workingSet.isEmpty()) {
    //            ListIterator<DyadicTupleRange> rangeIt = workingSet.listIterator();
    //
    //            while (rangeIt.hasNext()) {
    //                DyadicTupleRange range = rangeIt.next();
    //                long startIdx = (range.getStart() >> level) + (1l << (maxLevel - level));
    //                Double coeff = findCoeffValue(it, startIdx)
    //                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, level);
    //                if (range.getEnd() - range.getStart() + 1 == (1l << level)) {
    //                    //range is a full dyadic subrange
    //                    rangeIt.remove();
    //                    Double rangeValue = 0.0;
    //                    if (level == 0) {
    //                        rangeValue = ((startIdx & 0x1) == 0) ? range.getValue() + coeff : range.getValue() - coeff;
    //                    } else {
    //                        rangeValue = range.getValue() * (1l << level);
    //                    }
    //                    value += rangeValue;
    //                } else {
    //                    long startPos = (range.getStart() >> (level - 1)) + (1l << (maxLevel - level + 1));
    //                    long endPos = (range.getEnd() >> (level - 1)) + (1l << (maxLevel - level + 1));
    //                    long splitPos = (endPos - (1l << (maxLevel - level + 1))) << (level - 1);
    //                    //split the range
    //                    if (startPos != endPos) {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange(range.getStart(), splitPos - 1, range.getValue() + coeff));
    //                        rangeIt.add(new DyadicTupleRange(splitPos, range.getEnd(), range.getValue() - coeff));
    //                    } else {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange(range.getStart(), range.getEnd(),
    //                                range.getValue() + coeff * (((startPos & 0x1) == 0) ? 1 : -1)));
    //                    }
    //                }
    //            }
    //            level--;
    //        }
    //        return value;
    //    }

    @Override
    // Follows the algorithm from "Approximate computation of multidimensional aggregates of sparse data using wavelets"
    // Only coefficients along the path from root to leaves, designating the borders of the range, contribute to result
    // Contribution of non-root individual coefficient is calculated as (|left_leaves_i| - |right_leaves_i|) * C_i,
    // where |left_leaves_i| (|right_leaves_i| respectively) is the intersection between a set of leaves in the left
    // (right) subtree and a given query range [start, end]
    public double rangeQuery(long startPosition, long endPosition) {
        int level = 0;
        WaveletCoefficient leftBorderCoeff = new WaveletCoefficient(-1.0, level, startPosition);
        WaveletCoefficient rightBorderCoeff = new WaveletCoefficient(-1.0, level, endPosition);
        double result = 0.0;
        // relies on the fact that coefficients are sorted in ascending order of their *index* values (i.e. c0, c1, ...)
        ReverseListIterator<WaveletCoefficient> it =
                new ReverseListIterator<>((List<WaveletCoefficient>) synopsisElements);
        WaveletCoefficient currCoeff;
        if (it.hasNext()) {
            currCoeff = it.next();
            int coeffLevel = WaveletCoefficient.getLevel(currCoeff.getIdx(), maxLevel);
            while (level <= maxLevel) {
                while (coeffLevel <= level && it.hasNext()) {
                    if (currCoeff.getIdx() == leftBorderCoeff.getIdx()
                            || currCoeff.getIdx() == rightBorderCoeff.getIdx()) {
                        DyadicTupleRange supportInterval =
                                currCoeff.convertCoeffToSupportInterval(domainStart, domainEnd, maxLevel);
                        //border between left child's range and right child's range
                        long border = MathUtils.safeAverage(supportInterval.getStart(), supportInterval.getEnd());
                        long leftLeavesNum =
                                intersectInterval(supportInterval.getStart(), border, startPosition, endPosition);
                        long rightLeavesNum =
                                intersectInterval(border + 1, supportInterval.getEnd(), startPosition, endPosition);
                        result +=
                                currCoeff.getValue() * (leftLeavesNum - rightLeavesNum)
                                        * (normalize
                                                ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeffLevel)
                                                : 1);
                    }
                    currCoeff = it.next();
                    coeffLevel = WaveletCoefficient.getLevel(currCoeff.getIdx(), maxLevel);
                }
                level++;
                leftBorderCoeff.reset(leftBorderCoeff.getValue(), level,
                        leftBorderCoeff.getParentCoeffIndex(domainStart, maxLevel));
                rightBorderCoeff.reset(rightBorderCoeff.getValue(), level,
                        rightBorderCoeff.getParentCoeffIndex(domainStart, maxLevel));
            }
            // will this ever happen????
            while (it.hasNext() && currCoeff.getIdx() != 0) {
                currCoeff = it.next();
            }
            // Compute root coefficient's contribution
            if (currCoeff.getIdx() == 0) {
                result += MathUtils.safeRangeMultiply(endPosition, startPosition, currCoeff.getValue())
                        * (normalize ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeffLevel) : 1);
            }
        }
        return result;
    }

    private long intersectInterval(long i1Start, long i1End, long i2Start, long i2End) {
        if (i2Start > i1End && i2Start > i1Start || i1Start > i2End && i1End >= i2End) {
            return 0;
        } else {
            return Math.min(i2End, i1End) - Math.max(i1Start, i2Start) + 1;
        }
    }

    // Method implements creates a joint synopsis from two input wavelets. Result of the join is equivalent to synopsis,
    // built on the result of relational join between datasets, that were used to create left and right synopses
    public void join(WaveletSynopsis left, WaveletSynopsis right) {
        Map<Long, Double> joinedWavelet = new HashMap<>();

        for (WaveletCoefficient leftCoeff : left.getElements()) {
            for (WaveletCoefficient rightCoeff : right.getElements()) {
                int levelDifference = Math.abs(rightCoeff.getLevel() - leftCoeff.getLevel());
                if (levelDifference != 0) {
                    WaveletCoefficient larger = leftCoeff.getLevel() > rightCoeff.getLevel() ? leftCoeff : rightCoeff;
                    WaveletCoefficient smaller = leftCoeff.getLevel() > rightCoeff.getLevel() ? rightCoeff : leftCoeff;
                    // process only coefficients on the path from root to smaller
                    if (larger.getIdx() == smaller.getAncestorCoeffIndex(domainStart, maxLevel, levelDifference)) {
                        //main average always contributes positively
                        int sign = larger.getIdx() == 0 ? 1
                                : (((1L << (levelDifference - 1)) & smaller.getIdx()) > 0 ? -1 : 1);
                        double joinedCoeffValue = smaller.getValue() * larger.getValue() * sign * (!isNormalized() ? 1
                                : WaveletCoefficient.getNormalizationCoefficient(maxLevel, smaller.getLevel())
                                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, larger.getLevel()));
                        joinedWavelet.compute(smaller.getIdx(), (k, v) -> (v == null ? 0 : v) + joinedCoeffValue);
                    }
                } else if (leftCoeff.getIdx() == rightCoeff.getIdx()) {
                    WaveletCoefficient coeff = leftCoeff;
                    //special case: product of two main averages contributes to joined main average
                    if (coeff.getIdx() == 0) {
                        double joinedCoeffValue = leftCoeff.getValue() * rightCoeff.getValue() * (!isNormalized() ? 1
                                : WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeff.getLevel())
                                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeff.getLevel()));
                        joinedWavelet.compute(0L, (k, v) -> (v == null ? 0 : v) + joinedCoeffValue);
                    }
                    for (int i = maxLevel - coeff.getLevel() + 1; i > 0; i--) {
                        long ancestorIndex = coeff.getAncestorCoeffIndex(domainStart, maxLevel, i);
                        //main average always contributes positively
                        int sign = ancestorIndex == 0 ? 1 : (((1L << (i - 1)) & coeff.getIdx()) > 0 ? -1 : 1);
                        long scaleFactor = Math.min(i, maxLevel - coeff.getLevel());
                        // offset numeric overflow, which can drive (1L << scaleFactor) into negative domain
                        int adjustSign = (scaleFactor == Long.SIZE - 1) ? -1 : 1;
                        double joinedCoeffValue = sign * adjustSign * (!isNormalized() ? 1
                                : WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeff.getLevel())
                                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, coeff.getLevel()))
                                * ((coeff.getValue() * rightCoeff.getValue()) / (1L << scaleFactor));
                        joinedWavelet.compute(ancestorIndex, (k, v) -> (v == null ? 0 : v) + joinedCoeffValue);
                    }
                }
            }
        }
        for (Map.Entry<Long, Double> e : joinedWavelet.entrySet()) {
            addElement(e.getKey(), e.getValue(), WaveletCoefficient.getLevel(e.getKey(), maxLevel), normalize);
        }
    }
}
