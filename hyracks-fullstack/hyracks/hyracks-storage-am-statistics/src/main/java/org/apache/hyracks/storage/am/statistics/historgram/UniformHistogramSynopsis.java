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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UniformHistogramSynopsis extends EquiHeightHistogramSynopsis<UniformHistogramBucket> {

    private static final long serialVersionUID = 1L;
    private long lastAppendedTuple;

    public UniformHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum, int bucketsNum,
            List<UniformHistogramBucket> buckets) {
        super(domainStart, domainEnd, maxLevel, elementsNum, bucketsNum, buckets, null, null, null, null);
    }

    public UniformHistogramSynopsis(long domainStart, long domainEnd, int maxLevel, long elementsNum, int bucketsNum) {
        this(domainStart, domainEnd, maxLevel, elementsNum, bucketsNum, new ArrayList<>(bucketsNum));
    }

    @Override
    public void appendToBucket(int bucketId, int bucketNum, long tuple, double frequency) {
        if (bucketId >= bucketNum) {
            lastAppendedTuple = tuple;
            getBuckets().add(new UniformHistogramBucket(0l, frequency, 1));
        } else {
            if (tuple != lastAppendedTuple) {
                lastAppendedTuple = tuple;
                getBuckets().get(bucketId).incUniqueElementsNum();
            }
            getBuckets().get(bucketId).appendToValue(frequency);
        }
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.UniformHistogram;
    }

    @Override
    public double approximateValueWithinBucket(int bucketIdx, long start, long end) {
        double k = (double) getBucketSpan(bucketIdx) / getBuckets().get(bucketIdx).getUniqueElementsNum();
        return Math.round(((double) (end - start) / k) * getBuckets().get(bucketIdx).getValue()
                / (double) getBuckets().get(bucketIdx).getUniqueElementsNum());
    }

    @Override
    public Set<Long> getUnique() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<Integer, Byte> getSparseMap() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long[] getWordsAr() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setWordsAr(long[] words) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setSparse(Map<Integer, Byte> map) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setUnique(Set<Long> set) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setSparseMap(Map<Integer, Byte> map) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setUniqueSet(Set<Long> set) {
        // TODO Auto-generated method stub

    }
}
