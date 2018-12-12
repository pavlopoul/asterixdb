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
package org.apache.asterix.metadata.entities;

import java.util.Objects;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class Statistics implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    public final static String MERGED_STATS_ID = "";

    private final String dataverse;
    private final String dataset;
    private final String index;
    private final String field;
    private final String node;
    private final String partition;
    private final boolean isAntimatter;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Statistics that = (Statistics) o;
        return temp == that.temp && Objects.equals(dataverse, that.dataverse) && Objects.equals(dataset, that.dataset)
                && Objects.equals(index, that.index) && Objects.equals(field, that.field)
                && Objects.equals(node, that.node) && Objects.equals(partition, that.partition)
                && Objects.equals(componentID, that.componentID) && Objects.equals(synopsis, that.synopsis)
                && isAntimatter == that.isAntimatter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverse, dataset, index, field, node, partition, componentID, temp, isAntimatter,
                synopsis);
    }

    private final ComponentStatisticsId componentID;
    private final boolean temp;
    private final ISynopsis<? extends ISynopsisElement<Long>> synopsis;

    public Statistics(String dataverse, String dataset, String index, String field, String node, String partition,
            ComponentStatisticsId componentID, boolean temp, boolean isAntimatter,
            ISynopsis<? extends ISynopsisElement<Long>> synopsis) {
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
        this.field = field;
        this.node = node;
        this.partition = partition;
        this.componentID = componentID;
        this.temp = temp;
        this.isAntimatter = isAntimatter;
        this.synopsis = synopsis;
    }

    public String getDataverseName() {
        return dataverse;
    }

    public String getDatasetName() {
        return dataset;
    }

    public String getIndexName() {
        return index;
    }

    public String getFieldName() {
        return field;
    }

    public ComponentStatisticsId getComponentID() {
        return componentID;
    }

    public String getNode() {
        return node;
    }

    public String getPartition() {
        return partition;
    }

    public boolean isTemp() {
        return temp;
    }

    public boolean isAntimatter() {
        return isAntimatter;
    }

    public ISynopsis<? extends ISynopsisElement<Long>> getSynopsis() {
        return synopsis;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addStatisticsIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropStatistics(this);
    }
}
