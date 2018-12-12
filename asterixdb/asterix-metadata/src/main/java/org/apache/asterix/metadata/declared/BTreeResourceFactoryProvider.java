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
package org.apache.asterix.metadata.declared;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.StatisticsProperties;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.FilesIndexDescription;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetStatisticsHint;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.statistics.StatisticsUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.statistics.common.StatisticsFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;

public class BTreeResourceFactoryProvider implements IResourceFactoryProvider {

    public static final BTreeResourceFactoryProvider INSTANCE = new BTreeResourceFactoryProvider();

    private BTreeResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        int[] filterFields = IndexUtil.getFilterFields(dataset, index, filterTypeTraits);
        int[] btreeFields = IndexUtil.getBtreeFieldsIfFiltered(dataset, index);
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        String statisticsFieldsHint = dataset.getHints().get(DatasetStatisticsHint.NAME);
        SynopsisType statisticsType = getStatsType(mdProvider.getConfig(),
                mdProvider.getApplicationContext().getStatisticsProperties().getStatisticsSynopsisType());
        String[] unorderedStatisticsFields = null;
        if (!statisticsType.needsSortedOrder() && statisticsFieldsHint != null) {
            unorderedStatisticsFields = statisticsFieldsHint.split(",");
        }
        ITypeTraitProvider typeTraitProvider = mdProvider.getStorageComponentProvider().getTypeTraitProvider();
        ITypeTraits[] typeTraits = getTypeTraits(typeTraitProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories = getCmpFactories(mdProvider, dataset, index, recordType, metaType);
        int[] bloomFilterFields = getBloomFilterFields(dataset, index);
        double bloomFilterFalsePositiveRate = mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                return index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))
                        ? new ExternalBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories,
                                filterTypeTraits, filterCmpFactories, filterFields, opTrackerFactory,
                                ioOpCallbackFactory, metadataPageManagerFactory, ioSchedulerProvider,
                                mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                                bloomFilterFalsePositiveRate, false, btreeFields)
                        : new ExternalBTreeWithBuddyLocalResourceFactory(storageManager, typeTraits, cmpFactories,
                                filterTypeTraits, filterCmpFactories, filterFields, opTrackerFactory,
                                ioOpCallbackFactory, metadataPageManagerFactory, ioSchedulerProvider,
                                mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                                bloomFilterFalsePositiveRate, false, btreeFields);
            case INTERNAL:
                AsterixVirtualBufferCacheProvider vbcProvider =
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());

                final ICompressorDecompressorFactory compDecompFactory;
                if (index.isPrimaryIndex()) {
                    //Compress only primary index
                    compDecompFactory = mdProvider.getCompressionManager().getFactory(dataset.getCompressionScheme());
                } else {
                    compDecompFactory = NoOpCompressorDecompressorFactory.INSTANCE;
                }

                IStatisticsFactory statisticsFactory = null;
                if (statisticsType != SynopsisType.None) {
                    int statsSize = getStatsSize(mdProvider.getConfig(),
                            mdProvider.getApplicationContext().getStatisticsProperties().getStatisticsSize());
                    boolean statsOnPrimaryKeys = isStatsOnPrimaryKeysEnabled(mdProvider.getConfig(), mdProvider
                            .getApplicationContext().getStatisticsProperties().isStatisticsOnPrimaryKeysEnabled());
                    statisticsFactory = new StatisticsFactory(statisticsType, dataset.getDataverseName(),
                            dataset.getDatasetName(), index.getIndexName(),
                            StatisticsUtil.computeStatisticsFieldExtractors(typeTraitProvider, recordType,
                                    index.getKeyFieldNames(), index.isPrimaryIndex(), statsOnPrimaryKeys,
                                    unorderedStatisticsFields),
                            statsSize, mdProvider.getApplicationContext().getStatisticsProperties().getSketchFanout(),
                            mdProvider.getApplicationContext().getStatisticsProperties().getSketchFailureProbability(),
                            mdProvider.getApplicationContext().getStatisticsProperties().getSketchAccuracy(),
                            mdProvider.getApplicationContext().getStatisticsProperties().getSketchEnergyAccuracy());
                }
                return new LSMBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                        filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory,
                        metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, mergePolicyFactory,
                        mergePolicyProperties, true, bloomFilterFields, bloomFilterFalsePositiveRate,
                        index.isPrimaryIndex(), btreeFields, compDecompFactory, statisticsFactory,
                        mdProvider.getStorageComponentProvider().getStatisticsManagerProvider());
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                        dataset.getDatasetType().toString());
        }
    }

    private boolean isStatsOnPrimaryKeysEnabled(Map<String, Object> queryConfig, boolean statsOnPrimaryKeysEnabled) {
        // query-defined properties take precedence over config-defined
        String queryDefinedStatsSize = (String) queryConfig.get(StatisticsProperties.STATISTICS_PRIMARY_KEYS_ENABLED);
        if (queryDefinedStatsSize != null) {
            return Boolean.valueOf(queryDefinedStatsSize);
        }
        return statsOnPrimaryKeysEnabled;
    }

    private SynopsisType getStatsType(Map<String, Object> queryConfig, SynopsisType statsType) {
        // query-defined properties take precedence over config-defined
        String queryDefinedStatsSize = (String) queryConfig.get(StatisticsProperties.STATISTICS_SYNOPSIS_TYPE_KEY);
        if (queryDefinedStatsSize != null) {
            try {
                return SynopsisType.valueOf(queryDefinedStatsSize);
            } catch (IllegalArgumentException e) {
                //swallow, fall back to config value
            }
        }
        return statsType;
    }

    private int getStatsSize(Map<String, Object> queryConfig, int configDefinedStatsSize) {
        // query-defined properties take precedence over config-defined
        String queryDefinedStatsSize = (String) queryConfig.get(StatisticsProperties.STATISTICS_SYNOPSIS_SIZE_KEY);
        if (queryDefinedStatsSize != null) {
            try {
                return Integer.parseInt(queryDefinedStatsSize);
            } catch (NumberFormatException e) {
                //swallow, fall back to config value
            }
        }
        return configDefinedStatsSize;
    }

    public static ITypeTraits[] getTypeTraits(ITypeTraitProvider typeTraitProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(typeTraitProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return primaryTypeTraits;
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();

        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryTypeTraits[numSecondaryKeys + i] = primaryTypeTraits[i];
        }
        return secondaryTypeTraits;
    }

    public static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactory[] primaryCmpFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.FILES_INDEX_COMP_FACTORIES;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        IBinaryComparatorFactory[] secondaryCmpFactories =
                new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryCmpFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryCmpFactories[numSecondaryKeys + i] = primaryCmpFactories[i];
        }
        return secondaryCmpFactories;
    }

    public static int[] getBloomFilterFields(Dataset dataset, Index index) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return dataset.getPrimaryBloomFilterFields();
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            if (index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
                return FilesIndexDescription.BLOOM_FILTER_FIELDS;
            } else {
                return new int[] { index.getKeyFieldNames().size() };
            }
        }
        int numKeys = index.getKeyFieldNames().size();
        int[] bloomFilterKeyFields = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            bloomFilterKeyFields[i] = i;
        }
        return bloomFilterKeyFields;
    }
}
