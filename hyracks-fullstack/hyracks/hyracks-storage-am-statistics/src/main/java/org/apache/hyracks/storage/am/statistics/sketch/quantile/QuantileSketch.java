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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hyracks.storage.am.statistics.sketch.ISketch;

import com.google.common.collect.Iterators;

/**
 * Implementation of modified adaptive Greenwald-Khanna sketch from "Quantiles over data streams: An Experimental Study"
 */
public class QuantileSketch<T extends Comparable<T>> implements ISketch<T, T> {

    class QuantileSketchElement implements Comparable<QuantileSketchElement> {
        private final T value;
        private long g;
        private final long delta;

        public QuantileSketchElement(T value, long g, long delta) {
            this.value = value;
            this.g = g;
            this.delta = delta;
        }

        @Override
        public int compareTo(QuantileSketchElement o) {
            if (this == o)
                return 0;
            else
                return 1;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("V=").append(value).append(", g=").append(g).append(", Δ=").append(delta)
                    .toString();
        }
    }

    class ThresholdEntry {
        private QuantileSketchElement sketchElement;
        private double threshold;

        public ThresholdEntry(QuantileSketchElement sketchElement, double threshold) {
            this.sketchElement = sketchElement;
            this.threshold = threshold;
        }
    }

    class TreeMapWithDuplicates<K extends Comparable<K>, V> {
        private final TreeMap<K, LinkedList<V>> map;

        TreeMapWithDuplicates(V dummyMaxValue) {
            // use special comparator which treats null as the maximum value
            map = new TreeMap<>((o1, o2) -> {
                if (o1 == null) {
                    return o2 == null ? 0 : 1;
                } else if (o2 == null) {
                    return -1;
                } else {
                    return o1.compareTo(o2);
                }
            });
            // add dummy max value with key=null
            put(null, dummyMaxValue);
        }

        public int size() {
            return map.size();
        }

        public void put(K key, V value) {
            if (!map.containsKey(key)) {
                map.put(key, new LinkedList<>(Arrays.asList(value)));
            } else {
                map.get(key).add(value);
            }

        }

        /**
         * @param key
         * @return Returns a lowest element, which is greater than provided @key
         *         null, if the element is domain maximum
         */
        public V higherEntry(K key) {
            Entry<K, LinkedList<V>> successor = map.higherEntry(key);
            if (successor == null) {
                return null;
            }
            return successor.getValue().getFirst();
        }

        public V lastEntry() {
            // because we designate null as the last value, look for the highest entry less then dummy maximum
            Entry<K, LinkedList<V>> lastNonNullEntry = map.lowerEntry(null);
            if (lastNonNullEntry == null) {
                // map does not contain anything besides dummy maximum value null
                return null;
            }
            return map.lowerEntry(null).getValue().getLast();
        }

        public boolean containsKey(K key) {
            return map.containsKey(key);
        }

        public V next(K key, V prevValue) {
            V value;
            if (map.get(key).size() > 1 && !map.get(key).getLast().equals(prevValue)) {
                Iterator<V> it = map.get(key).iterator();
                while (it.hasNext() && !it.next().equals(prevValue));
                value = it.next();
            } else {
                value = map.higherEntry(key).getValue().getFirst();
            }
            return value;
        }

        public void remove(K key, V value) {
            if (map.get(key).size() > 1) {
                map.get(key).remove(value);
            } else {
                map.remove(key);
            }
        }

        public Iterator<V> iterator() {
            List<Iterator<V>> iterators = new ArrayList<>();
            for (List<V> list : map.values()) {
                iterators.add(list.iterator());
            }
            return Iterators.concat(iterators.iterator());
        }
    }

    private final int quantileNum;
    private final T domainStart;
    private final double accuracy;
    private final TreeMapWithDuplicates<T, QuantileSketchElement> elements;
    private final Queue<ThresholdEntry> compressibleElements;
    private int size;

    public QuantileSketch(int quantileNum, T domainStart, double accuracy) {
        this.quantileNum = quantileNum;
        this.domainStart = domainStart;
        this.accuracy = accuracy;
        elements = new TreeMapWithDuplicates<>(new QuantileSketchElement(null, 1, 0));
        //min heap to store elements thresholds
        compressibleElements = new PriorityQueue<>(Comparator.comparingDouble(o -> o.threshold));
    }

    @Override
    public int getSize() {
        return elements.size();
    }

    public int length() {
        return size;
    }

    @Override
    public void insert(T v) {
        QuantileSketchElement newElement;
        ThresholdEntry newElementThreshold;
        size++;
        long threshold = (long) Math.floor(accuracy * size * 2);
        // find successor quantile element
        QuantileSketchElement successor = elements.higherEntry(v);
        long newDelta = successor.g + successor.delta;
        if (newDelta <= threshold) {
            // merge new element into successor right away. Since new element is (v,1,Δ) successor's g is incremented.
            // Don't update compressibleElements, resolve discrepancy later.
            successor.g++;
            return;
        } else {
            newElement = new QuantileSketchElement(v, 1, newDelta - 1);
            // add entry to priority queue. Entry's threshold is calculated as gi + gi+1 + Δ = newDelta + 1
            newElementThreshold = new ThresholdEntry(newElement, newDelta + 1);
            elements.put(v, newElement);
            compressibleElements.offer(newElementThreshold);
        }
        while (true) {
            ThresholdEntry minElement = compressibleElements.peek();
            if (minElement.threshold > threshold) {
                // all elements are greater than threshold. simply break because the new element was already added
                break;
            }
            // update next element's threshold
            minElement = compressibleElements.poll();
            // find element next to minElement
            QuantileSketchElement nextElement = elements.next(minElement.sketchElement.value, minElement.sketchElement);
            // recalculate threshold value, because of the introduced by all entries merged without updating
            long newThreshold = minElement.sketchElement.g + nextElement.g + nextElement.delta;
            if (newThreshold <= threshold) {
                // merge minElement into nextElement
                nextElement.g += minElement.sketchElement.g;
                elements.remove(minElement.sketchElement.value, minElement.sketchElement);
                break;
            } else {
                // eliminate discrepancy
                minElement.threshold = newThreshold;
                compressibleElements.offer(minElement);
            }
        }
    }

    // returns max_i (gi+Δi) after all elements were added to the quantile summary
    public long calculateMaxError() {
        long maxError = 0;
        Iterator<QuantileSketchElement> it = elements.iterator();
        while (it.hasNext()) {
            QuantileSketchElement e = it.next();
            if (e.g + e.delta > maxError) {
                maxError = e.g + e.delta;
            }
        }
        return maxError / 2;
    }

    @Override
    public List<T> finish() {
        long maxError = calculateMaxError();
        List<T> ranks = new ArrayList<>(quantileNum);
        Iterator<QuantileSketchElement> it = elements.iterator();
        int quantile = 1;
        long nextRank = (long) Math.ceil(((double) size) / quantileNum);
        QuantileSketchElement prev = null;
        QuantileSketchElement e = it.hasNext() ? it.next() : null;
        if (e == null) {
            return ranks;
        }
        long rMin = e.g;
        while (it.hasNext() && quantile <= quantileNum) {
            boolean getNextRank = false;
            // if the requested rank is withing error bounds from the end
            //            if (nextRank > size - maxError) {
            //                // the largest value is an acceptable answer
            //                ranks.add(elements.lastEntry().value);
            //                getNextRank = true;
            //            } else
            if (rMin + e.delta > nextRank + maxError) {
                if (prev != null) {
                    ranks.add(prev.value);
                } else {
                    ranks.add(domainStart);
                }
                getNextRank = true;
            }
            if (getNextRank) {
                nextRank = (long) Math.ceil(((double) size * ++quantile) / quantileNum);
                continue;
            }
            prev = e;
            e = it.next();
            rMin += e.g;
        }
        if (quantile == 1) {
            ranks.add(prev.value);
        }
        // edge case for last quantile
        if (quantile == quantileNum) {
            ranks.add(elements.lastEntry().value);
        }
        if (prev == null) {
            // add the last element, since it marks the element with rank = 1
            ranks.add(e.value);
        }
        return ranks;
    }

}
