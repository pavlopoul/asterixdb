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

package org.apache.asterix.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

/**
 * Caches metadata entities such that the MetadataManager does not have to
 * contact the MetadataNode. The cache is updated transactionally via logical
 * logging in the MetadataTransactionContext. Note that transaction abort is
 * simply ignored, i.e., updates are not not applied to the cache.
 */
public class MetadataCache {

    // Key is dataverse name.
    protected final Map<DataverseName, Dataverse> dataverses = new HashMap<>();
    // Key is dataverse name. Key of value map is dataset name.
    protected final Map<DataverseName, Map<String, Dataset>> datasets = new HashMap<>();
    // Key is dataverse name. Key of value map is dataset name. Key of value map of value map is index name.
    protected final Map<DataverseName, Map<String, Map<String, Index>>> indexes = new HashMap<>();
    // Key is dataverse name. Key of value map is datatype name.
    protected final Map<DataverseName, Map<String, Datatype>> datatypes = new HashMap<>();
    // Key is node group name.
    protected final Map<String, NodeGroup> nodeGroups = new HashMap<>();
    // Key is function Identifier . Key of value map is function name.
    protected final Map<FunctionSignature, Function> functions = new HashMap<>();
    // Key is adapter dataverse name. Key of value map is the adapter name
    protected final Map<DataverseName, Map<String, DatasourceAdapter>> adapters = new HashMap<>();

    // Key is DataverseName, Key of the value map is the Policy name
    protected final Map<DataverseName, Map<String, FeedPolicyEntity>> feedPolicies = new HashMap<>();
    // Key is library dataverse. Key of value map is the library name
    protected final Map<DataverseName, Map<String, Library>> libraries = new HashMap<>();
    // Key is library dataverse. Key of value map is the feed name
    protected final Map<DataverseName, Map<String, Feed>> feeds = new HashMap<>();
    // Key is DataverseName, Key of the value map is the Policy name
    protected final Map<DataverseName, Map<String, CompactionPolicy>> compactionPolicies = new HashMap<>();
    // Key is DataverseName, Key of value map is feedConnectionId
    protected final Map<DataverseName, Map<String, FeedConnection>> feedConnections = new HashMap<>();
    // Key is synonym dataverse. Key of value map is the synonym name
    protected final Map<DataverseName, Map<String, Synonym>> synonyms = new HashMap<>();
    // Key is dataverse name. Key of value map is dataset name. Key of value map of value map is index name.
    protected final Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>>> statistics =
            new HashMap<>();

    // Atomically executes all metadata operations in ctx's log.
    public void commit(MetadataTransactionContext ctx) {
        // Forward roll the operations written in ctx's log.
        int logIx = 0;
        List<MetadataLogicalOperation> opLog = ctx.getOpLog();
        try {
            for (logIx = 0; logIx < opLog.size(); logIx++) {
                doOperation(opLog.get(logIx));
            }
        } catch (Exception e) {
            // Undo operations.
            try {
                for (int i = logIx - 1; i >= 0; i--) {
                    undoOperation(opLog.get(i));
                }
            } catch (Exception e2) {
                // We encountered an error in undo. This case should never
                // happen. Our only remedy to ensure cache consistency
                // is to clear everything.
                clear();
            }
        } finally {
            ctx.clear();
        }
    }

    public void clear() {
        synchronized (dataverses) {
            synchronized (nodeGroups) {
                synchronized (datasets) {
                    synchronized (indexes) {
                        synchronized (datatypes) {
                            synchronized (functions) {
                                synchronized (adapters) {
                                    synchronized (libraries) {
                                        synchronized (compactionPolicies) {
                                            synchronized (synonyms) {
                                                synchronized (statistics) {
                                                    dataverses.clear();
                                                    nodeGroups.clear();
                                                    datasets.clear();
                                                    indexes.clear();
                                                    datatypes.clear();
                                                    functions.clear();
                                                    adapters.clear();
                                                    libraries.clear();
                                                    compactionPolicies.clear();
                                                    synonyms.clear();
                                                    statistics.clear();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    public Dataverse addDataverseIfNotExists(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (datatypes) {
                    DataverseName dataverseName = dataverse.getDataverseName();
                    if (!dataverses.containsKey(dataverseName)) {
                        datasets.put(dataverseName, new HashMap<>());
                        datatypes.put(dataverseName, new HashMap<>());
                        adapters.put(dataverseName, new HashMap<>());
                        return dataverses.put(dataverseName, dataverse);
                    }
                    return null;
                }
            }
        }
    }

    public Dataset addDatasetIfNotExists(Dataset dataset) {
        synchronized (datasets) {
            synchronized (indexes) {
                // Add the primary index associated with the dataset, if the dataset is an
                // internal dataset.
                if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                    Index index = IndexUtil.getPrimaryIndex(dataset);
                    addIndexIfNotExistsInternal(index);
                }

                Map<String, Dataset> m = datasets.get(dataset.getDataverseName());
                if (m == null) {
                    m = new HashMap<>();
                    datasets.put(dataset.getDataverseName(), m);
                }
                if (!m.containsKey(dataset.getDatasetName())) {
                    return m.put(dataset.getDatasetName(), dataset);
                }
                return null;
            }
        }
    }

    public Index addIndexIfNotExists(Index index) {
        synchronized (indexes) {
            return addIndexIfNotExistsInternal(index);
        }
    }

    public Datatype addDatatypeIfNotExists(Datatype datatype) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(datatype.getDataverseName());
            if (m == null) {
                m = new HashMap<>();
                datatypes.put(datatype.getDataverseName(), m);
            }
            if (!m.containsKey(datatype.getDatatypeName())) {
                return m.put(datatype.getDatatypeName(), datatype);
            }
            return null;
        }
    }

    public NodeGroup addOrUpdateNodeGroup(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            return nodeGroups.put(nodeGroup.getNodeGroupName(), nodeGroup);
        }
    }

    public CompactionPolicy addCompactionPolicyIfNotExists(CompactionPolicy compactionPolicy) {
        synchronized (compactionPolicies) {
            Map<String, CompactionPolicy> p = compactionPolicies.get(compactionPolicy.getDataverseName());
            if (p == null) {
                p = new HashMap<>();
                p.put(compactionPolicy.getPolicyName(), compactionPolicy);
                compactionPolicies.put(compactionPolicy.getDataverseName(), p);
            } else {
                if (p.get(compactionPolicy.getPolicyName()) == null) {
                    p.put(compactionPolicy.getPolicyName(), compactionPolicy);
                }
            }
            return null;
        }
    }

    public Object addStatisticsIfNotExists(Statistics stats) {
        synchronized (statistics) {
            Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetMap =
                    statistics.get(stats.getDataverseName());
            if (datasetMap == null) {
                datasetMap = new HashMap<>();
                statistics.put(stats.getDataverseName(), datasetMap);
            }
            Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>> indexMap =
                    datasetMap.get(stats.getDatasetName());
            if (indexMap == null) {
                indexMap = new HashMap<>();
                datasetMap.put(stats.getDatasetName(), indexMap);
            }
            Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]> fieldMap =
                    indexMap.get(stats.getIndexName());
            if (fieldMap == null) {
                fieldMap = new HashMap<>();
                datasetMap.put(stats.getDatasetName(), indexMap);
            }
            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[] matterAntimatterArray =
                    fieldMap.get(stats.getFieldName());
            if (matterAntimatterArray == null) {
                matterAntimatterArray = new Map[2];
                fieldMap.put(stats.getFieldName(), matterAntimatterArray);
            }
            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>> nodeMap =
                    stats.isAntimatter() ? matterAntimatterArray[1] : matterAntimatterArray[0];
            if (nodeMap == null) {
                nodeMap = new HashMap<>();
                matterAntimatterArray[stats.isAntimatter() ? 1 : 0] = nodeMap;
                //create an empty nodeMap for the opposite synopsis to cache empty stats
                matterAntimatterArray[stats.isAntimatter() ? 0 : 1] = new HashMap<>();
            }
            Map<String, Map<ComponentStatisticsId, Statistics>> partitionMap = nodeMap.get(stats.getNode());
            if (partitionMap == null) {
                partitionMap = new HashMap<>();
                nodeMap.put(stats.getNode(), partitionMap);
            }
            Map<ComponentStatisticsId, Statistics> componentIdMap = partitionMap.get(stats.getPartition());
            if (componentIdMap == null) {
                componentIdMap = new HashMap<>();
                partitionMap.put(stats.getPartition(), componentIdMap);
            }

            if (!componentIdMap.containsKey(stats.getComponentID())) {
                return componentIdMap.put(stats.getComponentID(), stats);
            }
        }
        return null;
    }

    public CompactionPolicy dropCompactionPolicy(CompactionPolicy compactionPolicy) {
        synchronized (compactionPolicies) {
            Map<String, CompactionPolicy> p = compactionPolicies.get(compactionPolicy.getDataverseName());
            if (p != null && p.get(compactionPolicy.getPolicyName()) != null) {
                return p.remove(compactionPolicy.getPolicyName());
            }
            return null;
        }
    }

    public Dataverse dropDataverse(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (indexes) {
                    synchronized (datatypes) {
                        synchronized (functions) {
                            synchronized (adapters) {
                                synchronized (libraries) {
                                    synchronized (feeds) {
                                        synchronized (compactionPolicies) {
                                            synchronized (synonyms) {
                                                synchronized (statistics) {
                                                    datasets.remove(dataverse.getDataverseName());
                                                    indexes.remove(dataverse.getDataverseName());
                                                    datatypes.remove(dataverse.getDataverseName());
                                                    adapters.remove(dataverse.getDataverseName());
                                                    compactionPolicies.remove(dataverse.getDataverseName());
                                                    List<FunctionSignature> markedFunctionsForRemoval =
                                                            new ArrayList<>();
                                                    for (FunctionSignature signature : functions.keySet()) {
                                                        if (signature.getDataverseName()
                                                                .equals(dataverse.getDataverseName())) {
                                                            markedFunctionsForRemoval.add(signature);
                                                        }
                                                    }
                                                    for (FunctionSignature signature : markedFunctionsForRemoval) {
                                                        functions.remove(signature);
                                                    }
                                                    libraries.remove(dataverse.getDataverseName());
                                                    feeds.remove(dataverse.getDataverseName());
                                                    synonyms.remove(dataverse.getDataverseName());
                                                    statistics.remove(dataverse.getDataverseName());
                                                    return dataverses.remove(dataverse.getDataverseName());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public Dataset dropDataset(Dataset dataset) {
        synchronized (datasets) {
            synchronized (indexes) {
                synchronized (statistics) {

                    //remove the indexes of the dataset from indexes' cache
                    Map<String, Map<String, Index>> datasetMap = indexes.get(dataset.getDataverseName());
                    if (datasetMap != null) {
                        datasetMap.remove(dataset.getDatasetName());
                    }

                    //remove the index statistics of the dataset from statistics' cache
                    Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetStatsMap =
                            statistics.get(dataset.getDataverseName());
                    if (datasetStatsMap != null) {
                        datasetStatsMap.remove(dataset.getDatasetName());
                    }

                    //remove the dataset from datasets' cache
                    Map<String, Dataset> m = datasets.get(dataset.getDataverseName());
                    if (m == null) {
                        return null;
                    }
                    return m.remove(dataset.getDatasetName());
                }
            }
        }
    }

    public Index dropIndex(Index index) {
        synchronized (indexes) {
            synchronized (statistics) {
                //remove the index statistics of the dataset from statistics' cache
                Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetStatsMap =
                        statistics.get(index.getDataverseName());
                if (datasetStatsMap != null) {
                    Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>> indexStatsMap =
                            datasetStatsMap.get(index.getDatasetName());
                    if (indexStatsMap != null) {
                        indexStatsMap.remove(index.getIndexName());
                    }
                }

                Map<String, Map<String, Index>> datasetMap = indexes.get(index.getDataverseName());
                if (datasetMap == null) {
                    return null;
                }

                Map<String, Index> indexMap = datasetMap.get(index.getDatasetName());
                if (indexMap == null) {
                    return null;
                }
                return indexMap.remove(index.getIndexName());
            }
        }
    }

    public Datatype dropDatatype(Datatype datatype) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(datatype.getDataverseName());
            if (m == null) {
                return null;
            }
            return m.remove(datatype.getDatatypeName());
        }
    }

    public NodeGroup dropNodeGroup(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            return nodeGroups.remove(nodeGroup.getNodeGroupName());
        }
    }

    public Object dropStatistics(Statistics stat) {
        synchronized (statistics) {
            Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetMap =
                    statistics.get(stat.getDataverseName());
            if (datasetMap == null) {
                return null;
            }

            Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>> indexMap =
                    datasetMap.get(stat.getDatasetName());
            if (indexMap == null) {
                return null;
            }

            Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]> fieldMap =
                    indexMap.get(stat.getIndexName());
            if (fieldMap == null) {
                return null;
            }

            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[] matterAntimatterList =
                    fieldMap.get(stat.getFieldName());
            if (matterAntimatterList == null) {
                return null;
            }

            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>> nodeMap =
                    stat.isAntimatter() ? matterAntimatterList[1] : matterAntimatterList[0];
            if (nodeMap == null) {
                return null;
            }

            Map<String, Map<ComponentStatisticsId, Statistics>> partitionMap = nodeMap.get(stat.getNode());
            if (partitionMap == null) {
                return null;
            }

            Map<ComponentStatisticsId, Statistics> componentIdMap = partitionMap.get(stat.getPartition());
            if (componentIdMap == null) {
                return null;
            }
            return componentIdMap.remove(stat.getComponentID());
        }
    }

    public Dataverse getDataverse(DataverseName dataverseName) {
        synchronized (dataverses) {
            return dataverses.get(dataverseName);
        }
    }

    public Dataset getDataset(DataverseName dataverseName, String datasetName) {
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(datasetName);
        }
    }

    public Index getIndex(DataverseName dataverseName, String datasetName, String indexName) {
        synchronized (indexes) {
            Map<String, Map<String, Index>> datasetMap = indexes.get(dataverseName);
            if (datasetMap == null) {
                return null;
            }
            Map<String, Index> indexMap = datasetMap.get(datasetName);
            if (indexMap == null) {
                return null;
            }
            return indexMap.get(indexName);
        }
    }

    public Datatype getDatatype(DataverseName dataverseName, String datatypeName) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(datatypeName);
        }
    }

    public NodeGroup getNodeGroup(String nodeGroupName) {
        synchronized (nodeGroups) {
            return nodeGroups.get(nodeGroupName);
        }
    }

    public Function getFunction(FunctionSignature functionSignature) {
        synchronized (functions) {
            return functions.get(functionSignature);
        }
    }

    public List<Dataset> getDataverseDatasets(DataverseName dataverseName) {
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataverseName);
            if (m == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(m.values());
        }
    }

    public List<Index> getDatasetIndexes(DataverseName dataverseName, String datasetName) {
        synchronized (datasets) {
            Map<String, Index> map = indexes.get(dataverseName).get(datasetName);
            if (map == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(map.values());
        }
    }

    public Statistics getStatistics(String dataverseName, String datasetName, String indexName, String fieldName,
            String node, String partition, ComponentStatisticsId componentId, boolean isAntimatter) {
        synchronized (statistics) {
            Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetMap =
                    statistics.get(dataverseName);
            if (datasetMap == null) {
                return null;
            }

            Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>> indexMap =
                    datasetMap.get(datasetName);
            if (indexMap == null) {
                return null;
            }

            Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]> fieldMap =
                    indexMap.get(indexName);
            if (fieldMap == null) {
                return null;
            }

            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[] antimatterMap = fieldMap.get(fieldName);
            if (antimatterMap == null) {
                return null;
            }

            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>> nodeMap =
                    isAntimatter ? antimatterMap[1] : antimatterMap[0];
            if (nodeMap == null) {
                return null;
            }

            Map<String, Map<ComponentStatisticsId, Statistics>> partitionMap = nodeMap.get(node);
            if (partitionMap == null) {
                return null;
            }

            Map<ComponentStatisticsId, Statistics> componentIdMap = partitionMap.get(partition);
            if (componentIdMap == null) {
                return null;
            }
            return componentIdMap.get(componentId);
        }
    }

    public List<Statistics> getFieldStatistics(String dataverseName, String datasetName, String indexName,
            String fieldName, boolean isAntimatter) {
        synchronized (statistics) {
            List<Statistics> results = new ArrayList<>();

            Map<String, Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>>> datasetMap =
                    statistics.get(dataverseName);
            if (datasetMap == null) {
                return null;
            }
            Map<String, Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]>> indexMap =
                    datasetMap.get(datasetName);
            if (indexMap == null) {
                return null;
            }
            Map<String, Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[]> fieldMap =
                    indexMap.get(indexName);
            if (fieldMap == null) {
                return null;
            }
            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>>[] matterAntimatterList =
                    fieldMap.get(fieldName);
            if (matterAntimatterList == null) {
                return null;
            }
            Map<String, Map<String, Map<ComponentStatisticsId, Statistics>>> nodeMap =
                    isAntimatter ? matterAntimatterList[1] : matterAntimatterList[0];
            if (nodeMap == null) {
                return null;
            }

            for (Map.Entry<String, Map<String, Map<ComponentStatisticsId, Statistics>>> partitionMap : nodeMap
                    .entrySet()) {
                for (Map.Entry<String, Map<ComponentStatisticsId, Statistics>> componentIdMap : partitionMap.getValue()
                        .entrySet()) {
                    for (Map.Entry<ComponentStatisticsId, Statistics> statsMap : componentIdMap.getValue().entrySet()) {
                        results.add(statsMap.getValue());
                    }
                }
            }
            return results;
        }
    }

    protected void doOperation(MetadataLogicalOperation op) {
        if (op.isAdd) {
            op.entity.addToCache(this);
        } else {
            op.entity.dropFromCache(this);
        }
    }

    protected void undoOperation(MetadataLogicalOperation op) {
        if (!op.isAdd) {
            op.entity.addToCache(this);
        } else {
            op.entity.dropFromCache(this);
        }
    }

    public Function addFunctionIfNotExists(Function function) {
        synchronized (functions) {
            FunctionSignature signature =
                    new FunctionSignature(function.getDataverseName(), function.getName(), function.getArity());
            Function fun = functions.get(signature);
            if (fun == null) {
                return functions.put(signature, function);
            }
            return null;
        }
    }

    public Function dropFunction(Function function) {
        synchronized (functions) {
            FunctionSignature signature =
                    new FunctionSignature(function.getDataverseName(), function.getName(), function.getArity());
            Function fun = functions.get(signature);
            if (fun == null) {
                return null;
            }
            return functions.remove(signature);
        }
    }

    public Object addFeedPolicyIfNotExists(FeedPolicyEntity feedPolicy) {
        synchronized (feedPolicies) {
            Map<String, FeedPolicyEntity> p = feedPolicies.get(feedPolicy.getDataverseName());
            if (p == null) {
                p = new HashMap<>();
                p.put(feedPolicy.getPolicyName(), feedPolicy);
                feedPolicies.put(feedPolicy.getDataverseName(), p);
            } else {
                if (p.get(feedPolicy.getPolicyName()) == null) {
                    p.put(feedPolicy.getPolicyName(), feedPolicy);
                }
            }
            return null;
        }
    }

    public Object dropFeedPolicy(FeedPolicyEntity feedPolicy) {
        synchronized (feedPolicies) {
            Map<String, FeedPolicyEntity> p = feedPolicies.get(feedPolicy.getDataverseName());
            if (p != null && p.get(feedPolicy.getPolicyName()) != null) {
                return p.remove(feedPolicy.getPolicyName()).getPolicyName();
            }
            return null;
        }
    }

    public DatasourceAdapter addAdapterIfNotExists(DatasourceAdapter adapter) {
        synchronized (adapters) {
            Map<String, DatasourceAdapter> adaptersInDataverse =
                    adapters.get(adapter.getAdapterIdentifier().getDataverseName());
            if (adaptersInDataverse == null) {
                adaptersInDataverse = new HashMap<>();
                adapters.put(adapter.getAdapterIdentifier().getDataverseName(), adaptersInDataverse);
            }
            DatasourceAdapter adapterObject = adaptersInDataverse.get(adapter.getAdapterIdentifier().getName());
            if (adapterObject == null) {
                return adaptersInDataverse.put(adapter.getAdapterIdentifier().getName(), adapter);
            }
            return null;
        }
    }

    public DatasourceAdapter dropAdapterIfExists(DatasourceAdapter adapter) {
        synchronized (adapters) {
            Map<String, DatasourceAdapter> adaptersInDataverse =
                    adapters.get(adapter.getAdapterIdentifier().getDataverseName());
            if (adaptersInDataverse != null) {
                return adaptersInDataverse.remove(adapter.getAdapterIdentifier().getName());
            }
            return null;
        }
    }

    public Library addLibraryIfNotExists(Library library) {
        synchronized (libraries) {
            Map<String, Library> libsInDataverse = libraries.get(library.getDataverseName());
            boolean needToAdd = (libsInDataverse == null || libsInDataverse.get(library.getName()) != null);
            if (needToAdd) {
                if (libsInDataverse == null) {
                    libsInDataverse = new HashMap<>();
                    libraries.put(library.getDataverseName(), libsInDataverse);
                }
                return libsInDataverse.put(library.getName(), library);
            }
            return null;
        }
    }

    public Library dropLibrary(Library library) {
        synchronized (libraries) {
            Map<String, Library> librariesInDataverse = libraries.get(library.getDataverseName());
            if (librariesInDataverse != null) {
                return librariesInDataverse.remove(library.getName());
            }
            return null;
        }
    }

    public FeedConnection addFeedConnectionIfNotExists(FeedConnection feedConnection) {
        synchronized (feedConnections) {
            Map<String, FeedConnection> feedConnsInDataverse = feedConnections.get(feedConnection.getDataverseName());
            if (feedConnsInDataverse == null) {
                feedConnsInDataverse = new HashMap<>();
                feedConnections.put(feedConnection.getDataverseName(), feedConnsInDataverse);
            }
            return feedConnsInDataverse.put(feedConnection.getConnectionId(), feedConnection);
        }
    }

    public FeedConnection dropFeedConnection(FeedConnection feedConnection) {
        synchronized (feedConnections) {
            Map<String, FeedConnection> feedConnsInDataverse = feedConnections.get(feedConnection.getDataverseName());
            if (feedConnsInDataverse != null) {
                return feedConnsInDataverse.remove(feedConnection.getConnectionId());
            } else {
                return null;
            }
        }
    }

    public Feed addFeedIfNotExists(Feed feed) {
        synchronized (feeds) {
            Map<String, Feed> feedsInDataverse = feeds.get(feed.getDataverseName());
            if (feedsInDataverse == null) {
                feedsInDataverse = new HashMap<>();
                feeds.put(feed.getDataverseName(), feedsInDataverse);
            }
            return feedsInDataverse.put(feed.getFeedName(), feed);
        }
    }

    public Feed dropFeedIfExists(Feed feed) {
        synchronized (feeds) {
            Map<String, Feed> feedsInDataverse = feeds.get(feed.getDataverseName());
            if (feedsInDataverse != null) {
                return feedsInDataverse.remove(feed.getFeedName());
            }
            return null;
        }
    }

    public Synonym addSynonymIfNotExists(Synonym synonym) {
        synchronized (synonyms) {
            Map<String, Synonym> synonymsInDataverse = synonyms.get(synonym.getDataverseName());
            if (synonymsInDataverse == null) {
                synonymsInDataverse = new HashMap<>();
                synonyms.put(synonym.getDataverseName(), synonymsInDataverse);
            }
            return synonymsInDataverse.put(synonym.getSynonymName(), synonym);
        }
    }

    public Synonym dropSynonym(Synonym synonym) {
        synchronized (synonyms) {
            Map<String, Synonym> synonymsInDataverse = synonyms.get(synonym.getDataverseName());
            if (synonymsInDataverse != null) {
                return synonymsInDataverse.remove(synonym.getSynonymName());
            }
            return null;
        }
    }

    private Index addIndexIfNotExistsInternal(Index index) {
        Map<String, Map<String, Index>> datasetMap = indexes.get(index.getDataverseName());
        if (datasetMap == null) {
            datasetMap = new HashMap<>();
            indexes.put(index.getDataverseName(), datasetMap);
        }
        Map<String, Index> indexMap = datasetMap.get(index.getDatasetName());
        if (indexMap == null) {
            indexMap = new HashMap<>();
            datasetMap.put(index.getDatasetName(), indexMap);
        }
        if (!indexMap.containsKey(index.getIndexName())) {
            return indexMap.put(index.getIndexName(), index);
        }
        return null;
    }

    /**
     * Represents a logical operation against the metadata.
     */
    protected static class MetadataLogicalOperation {
        // Entity to be added/dropped.
        public final IMetadataEntity<?> entity;
        // True for add, false for drop.
        public final boolean isAdd;

        public MetadataLogicalOperation(IMetadataEntity<?> entity, boolean isAdd) {
            this.entity = entity;
            this.isAdd = isAdd;
        }
    }
}
