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
package org.apache.asterix.test.dataflow;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractStatisticsFactory;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class TestCountingStatisticsFactory extends AbstractStatisticsFactory {
    public static class CountingSynopsis extends AbstractSynopsis {

        public int getCount() {
            return count;
        }

        private int count = 0;

        public CountingSynopsis() {
            super(Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.SIZE, 0, null);
        }

        @Override
        public SynopsisType getType() {
            return null;
        }

        @Override
        public void merge(ISynopsis mergeSynopsis) throws HyracksDataException {

        }

        @Override
        public double pointQuery(long position) {
            return 0;
        }

        @Override
        public double rangeQuery(long startPosition, long endPosition) {
            return 0;
        }
    }

    class CountingSynopsisBuilder extends AbstractSynopsisBuilder {

        public CountingSynopsisBuilder(ISynopsis synopsis, String dataverse, String dataset, String index, String field,
                boolean isAntimatter, ComponentStatistics componentStatistics) {
            super(synopsis, dataverse, dataset, index, field, isAntimatter, componentStatistics);
        }

        @Override
        public void processTuple(ITupleReference tuple) throws HyracksDataException {
            ((CountingSynopsis) synopsis).count++;
        }

        @Override
        public void finishSynopsisBuild() throws HyracksDataException {
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

    private final String dataverse;
    private final String dataset;
    private final String index;

    public TestCountingStatisticsFactory(String dataverse, String dataset, String index,
            List<IFieldExtractor> fieldExtractors) {
        super(fieldExtractors, SynopsisType.None);
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
    }

    @Override
    protected AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, IFieldExtractor fieldExtractor) throws HyracksDataException {
        return new CountingSynopsisBuilder(new CountingSynopsis(), dataverse, dataset, index,
                fieldExtractor.getFieldName(), isAntimatter, componentStatistics);
    }

    @Override
    public boolean canCollectStats() {
        return true;
    }
}
