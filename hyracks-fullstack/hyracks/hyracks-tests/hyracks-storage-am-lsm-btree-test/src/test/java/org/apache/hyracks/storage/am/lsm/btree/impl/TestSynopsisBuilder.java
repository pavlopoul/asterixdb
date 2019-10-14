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

import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class TestSynopsisBuilder extends AbstractSynopsisBuilder<TestSynopsis> {
    private final IFieldExtractor<Comparable> fieldExtractor;

    TestSynopsisBuilder(TestSynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, ComponentStatistics componentStatistics, IFieldExtractor<Comparable> fieldExtractor) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, componentStatistics);
        this.fieldExtractor = fieldExtractor;
    }

    private Map<Comparable, Integer> elementsCardinality = new TreeMap<>();

    @Override
    protected void processTuple(ITupleReference tuple) throws HyracksDataException {
        Comparable value = fieldExtractor.extractFieldValue(tuple);
        elementsCardinality.put(value, elementsCardinality.getOrDefault(value, 0) + 1);
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        for (Map.Entry<Comparable, Integer> e : elementsCardinality.entrySet()) {
            synopsis.getElements().add(new TestSynopsisElement(e.getKey(), e.getValue()));
        }
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean hasFailed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Throwable getFailure() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}
