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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContinuousHistogramSynopsis extends EquiHeightHistogramSynopsis<HistogramBucket> {

    private static final long serialVersionUID = 1L;

    public ContinuousHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum,
            int bucketsNum) {
        this(domainStart, domainEnd, maxLevel, elementsNum, bucketsNum, new ArrayList<>(bucketsNum), new HashSet<>(),
                new HashMap<>(), new long[(int) (((5 * 1048576) + 63) >>> 6)]);
    }

    public ContinuousHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum, int bucketsNum,
            List<HistogramBucket> buckets, Set<Long> uniqueSet, Map<Integer, Byte> map, long[] words) {
        super(domainStart, domainEnd, maxLevel, elementsNum, bucketsNum, buckets, null, uniqueSet, map, words);
    }

    public ContinuousHistogramSynopsis(ContinuousHistogramSynopsis synopsis) {
        this(synopsis.getDomainStart(), synopsis.getDomainEnd(), synopsis.getMaxLevel(),
                synopsis.getElementsPerBucket(), synopsis.getBuckets().size(), synopsis.getBuckets(), synopsis.getSet(),
                synopsis.getSparse(), synopsis.getWords());
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.ContinuousHistogram;
    }

    public void appendToBucket(int bucketId, int bucketNum, long tuplePos, double frequency) {
        if (bucketId >= getBuckets().size()) {
            getBuckets().add(new HistogramBucket(0l, frequency, 0l, this.getElementsPerBucket()));
        } else {
            getBuckets().get(bucketId).appendToValue(frequency);
        }
    }

    public Set<Long> getUnique() {
        // TODO Auto-generated method stub
        return getSet();
    }

    public Map<Integer, Byte> getSparseMap() {
        return getSparse();
    }

    public long[] getWordsAr() {
        return getWords();
    }

    public void setWordsAr(long[] words) {
        setWords(words);
    }

    public void setSparseMap(Map<Integer, Byte> map) {
        setSparse(map);

    }

    public void setUniqueSet(Set<Long> set) {
        setUnique(set);

    }

}
