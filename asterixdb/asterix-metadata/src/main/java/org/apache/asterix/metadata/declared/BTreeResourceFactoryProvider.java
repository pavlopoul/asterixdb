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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.AqlOrdinalPrimitiveValueProviderFactory;
import org.apache.asterix.external.indexing.FilesIndexDescription;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetStatisticsHint;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.am.statistics.common.StatisticsCollectorFactory;
import org.apache.hyracks.storage.am.statistics.common.TupleFieldExtractor;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;

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
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType);
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
                return new LSMBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                        filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory,
                        metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, mergePolicyFactory,
                        mergePolicyProperties, true, bloomFilterFields, bloomFilterFalsePositiveRate,
                        index.isPrimaryIndex(), btreeFields,
                        prepareStatisticsFactory(mdProvider, dataset, index, recordType, typeTraits,
                                mdProvider.getApplicationContext().getStatisticsProperties()
                                        .getStatisticsSynopsisType()),
                        mdProvider.getStorageComponentProvider().getStatisticsManagerProvider());
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                        dataset.getDatasetType().toString());
        }
    }

    private IStatisticsFactory prepareStatisticsFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ITypeTraits[] typeTraits, SynopsisType statsType) throws AlgebricksException {
        if (statsType != null && statsType != SynopsisType.None) {
            // collect statistics on index key, which is always the first field
            List<String> statisticsFields =
                    index.getKeyFieldNames().stream().map(l -> String.join(".", l)).collect(Collectors.toList());
            int[] indexKeyFieldIds = IndexUtil.getSecondaryKeys(dataset, index);
            List<ITypeTraits> statisticsTypeTraits = new ArrayList<>();
            List<IFieldExtractor> statisticsFieldExtractors = new ArrayList<>();
            if (indexKeyFieldIds.length == 1) {
                statisticsTypeTraits.addAll(IndexUtil.getSecondaryKeyTypeTraits(indexKeyFieldIds, typeTraits));
                for (int indexKeyFieldId : indexKeyFieldIds) {
                    statisticsFieldExtractors.add(new TupleFieldExtractor(
                            AqlOrdinalPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider(),
                            indexKeyFieldId));
                }
            }
            // check statistics hint
            String statisticsFieldsHint = dataset.getHints().get(DatasetStatisticsHint.NAME);
            if (index.isPrimaryIndex() && !statsType.needsSortedOrder() && statisticsFieldsHint != null) {
                String[] unorderedStatisticsFields = statisticsFieldsHint.split(",");
                statisticsFields.addAll(Arrays.asList(unorderedStatisticsFields));
                int[] unorderedStatisticsFieldIds = TypeUtil.getFieldIds(recordType, unorderedStatisticsFields);
                for (int i = 0; i < unorderedStatisticsFields.length; i++) {
                    statisticsFieldExtractors.add(getFieldExtractor(recordType, unorderedStatisticsFieldIds[i]));
                }
                statisticsTypeTraits.addAll(DatasetUtil.computeFieldTypeTraits(unorderedStatisticsFields, recordType));
            }
            return new StatisticsCollectorFactory(statsType, dataset.getDataverseName(), dataset.getDatasetName(),
                    index.getIndexName(), statisticsFields, statisticsTypeTraits, statisticsFieldExtractors,
                    mdProvider.getApplicationContext().getStatisticsProperties().getStatisticsSize(),
                    mdProvider.getApplicationContext().getStatisticsProperties().getSketchFanout(),
                    mdProvider.getApplicationContext().getStatisticsProperties().getSketchFailureProbability(),
                    mdProvider.getApplicationContext().getStatisticsProperties().getSketchAccuracy(),
                    mdProvider.getApplicationContext().getStatisticsProperties().getSketchEnergyAccuracy());
        }
        return null;
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return primaryTypeTraits;
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
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

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
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

    private static int[] getBloomFilterFields(Dataset dataset, Index index) throws AlgebricksException {
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

    private IFieldExtractor getFieldExtractor(ARecordType recordType, int statisticsField) {
        final int hyracksFieldIdx = 1;
        return tuple -> {
            ARecordPointable recPointable = ARecordPointable.FACTORY.createPointable();
            ATypeTag tag = recPointable.getClosedFieldType(recordType, statisticsField).getTypeTag();
            if (tuple.getFieldCount() < hyracksFieldIdx) {
                throw new HyracksDataException(
                        "Cannot extract field " + hyracksFieldIdx + " from incoming hyracks tuple");
            }
            recPointable.set(tuple.getFieldData(hyracksFieldIdx), tuple.getFieldStart(hyracksFieldIdx),
                    tuple.getFieldLength(hyracksFieldIdx));
            return AqlOrdinalPrimitiveValueProviderFactory.getTaggedOrdinalValue(tag, recPointable.getByteArray(),
                    recPointable.getClosedFieldOffset(recordType, statisticsField));
        };
    }
}
