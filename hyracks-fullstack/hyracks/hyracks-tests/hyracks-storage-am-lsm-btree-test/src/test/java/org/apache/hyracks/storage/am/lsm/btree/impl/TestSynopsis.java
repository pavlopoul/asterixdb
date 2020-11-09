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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;

public class TestSynopsis implements ISynopsis<TestSynopsisElement> {

    protected Collection<TestSynopsisElement> elements;

    public TestSynopsis() {
        elements = new TreeSet<>();
    }

    @Override
    public int getSize() {
        return elements.size();
    }

    @Override
    public Collection<TestSynopsisElement> getElements() {
        return elements;
    }

    @Override
    public SynopsisType getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double pointQuery(long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double rangeQuery(long startPosition, long endPosition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(ISynopsis<TestSynopsisElement> mergeSynopsis) throws HyracksDataException {
        if (mergeSynopsis != null) {
            elements.addAll(mergeSynopsis.getElements());
        }
    }

    @Override
    public double joinQuery(ISynopsis synopsis, boolean primIndex) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Map<Long, Integer> getMap() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long uniqueQuery(boolean primIndex) {
        // TODO Auto-generated method stub
        return 0;
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
    public void mergeUnique(ISynopsis<TestSynopsisElement> mergeSynopsis) throws HyracksDataException {
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
