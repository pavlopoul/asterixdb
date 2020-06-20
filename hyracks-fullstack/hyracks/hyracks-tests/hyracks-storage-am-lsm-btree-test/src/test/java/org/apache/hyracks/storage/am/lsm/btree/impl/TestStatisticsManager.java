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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;

public class TestStatisticsManager implements IStatisticsManager {

    private MultiValuedMap<ILSMDiskComponent, TestStatisticsEntry> componentIndex = new ArrayListValuedHashMap<>();
    private MultiValuedMap<String, ISynopsis> synopsisIndex = new ArrayListValuedHashMap<>();
    private Set<ISynopsis> flushed = new HashSet<>();

    class TestStatisticsEntry {
        String fieldName;
        ISynopsis synopsis;

        public TestStatisticsEntry(String fieldName, ISynopsis synopsis) {
            this.fieldName = fieldName;
            this.synopsis = synopsis;
        }
    }

    public boolean isFlushed(ISynopsis s) {
        return flushed.contains(s);
    }

    @Override
    public void sendFlushStatistics(ILSMDiskComponent flushedComponent) throws HyracksDataException {
        for (TestStatisticsEntry e : componentIndex.get(flushedComponent)) {
            flushed.add(e.synopsis);
        }
    }

    @Override
    public void sendMergeStatistics(ILSMDiskComponent newComponent, List<ILSMDiskComponent> mergedComponents)
            throws HyracksDataException {
        for (ILSMDiskComponent c : mergedComponents) {
            for (TestStatisticsEntry removedEntry : componentIndex.remove(c)) {
                synopsisIndex.get(removedEntry.fieldName).remove(removedEntry.synopsis);
                flushed.remove(removedEntry.synopsis);
            }
        }
        for (TestStatisticsEntry e : componentIndex.get(newComponent)) {
            flushed.add(e.synopsis);
        }
    }

    @Override
    public void addStatistics(ISynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, ILSMDiskComponent component) {
        TestStatisticsEntry newEntry = new TestStatisticsEntry(field, synopsis);
        componentIndex.put(component, newEntry);
        synopsisIndex.put(field, synopsis);
    }

    public Collection<ISynopsis> getStatistics(String field) {
        return synopsisIndex.get(field);
    }
}
