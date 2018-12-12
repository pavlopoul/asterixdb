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

import java.util.Map;

import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.declared.BTreeResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.statistics.StatisticsUtil;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;

public class TestLsmBTreeResourceFactoryProvider implements IResourceFactoryProvider {

    private final boolean hasStatistics;

    public TestLsmBTreeResourceFactoryProvider(boolean hasStatistics) {
        this.hasStatistics = hasStatistics;
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        int[] filterFields = IndexUtil.getFilterFields(dataset, index, filterTypeTraits);
        int[] btreeFields = IndexUtil.getBtreeFieldsIfFiltered(dataset, index);
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        ITypeTraitProvider typeTraitProvider = storageComponentProvider.getTypeTraitProvider();
        ITypeTraits[] typeTraits =
                BTreeResourceFactoryProvider.getTypeTraits(typeTraitProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories =
                BTreeResourceFactoryProvider.getCmpFactories(mdProvider, dataset, index, recordType, metaType);
        int[] bloomFilterFields = BTreeResourceFactoryProvider.getBloomFilterFields(dataset, index);
        double bloomFilterFalsePositiveRate = mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        if (opTrackerFactory instanceof PrimaryIndexOperationTrackerFactory) {
            opTrackerFactory = new TestPrimaryIndexOperationTrackerFactory(dataset.getDatasetId());
        }
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        AsterixVirtualBufferCacheProvider vbcProvider = new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());
        return new TestLsmBtreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                bloomFilterFalsePositiveRate, index.isPrimaryIndex(), btreeFields,
                new TestCountingStatisticsFactory(dataset.getDataverseName(), dataset.getDatasetName(),
                        index.getIndexName(),
                        StatisticsUtil.computeStatisticsFieldExtractors(typeTraitProvider, recordType,
                                index.getKeyFieldNames(), index.isPrimaryIndex(), true, null)),
                hasStatistics ? storageComponentProvider.getStatisticsManagerProvider() : null);
    }
}
