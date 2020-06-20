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
package org.apache.asterix.metadata.entitytupletranslators;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;
import org.apache.hyracks.storage.am.statistics.common.SynopsisElementFactory;
import org.apache.hyracks.storage.am.statistics.common.SynopsisFactory;
import org.apache.hyracks.storage.am.statistics.historgram.UniformHistogramBucket;

public class StatisticsTupleTranslator extends AbstractTupleTranslator<Statistics> {
    private static final int STATISTICS_PAYLOAD_TUPLE_FIELD_INDEX = 8;
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.STATISTICS_RECORDTYPE);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private transient AMutableInt64 aInt64 = new AMutableInt64(0);
    private transient AMutableInt32 aInt32 = new AMutableInt32(0);
    private transient AMutableDouble aDouble = new AMutableDouble(0.0);

    private final MetadataNode metadataNode;
    private final TxnId txnId;

    public StatisticsTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.STATISTICS_DATASET, STATISTICS_PAYLOAD_TUPLE_FIELD_INDEX);
        this.txnId = txnId;
        this.metadataNode = metadataNode;
    }

    @Override
    public Statistics createMetadataEntityFromARecord(ARecord statisticsRecord) {

        String dataverseCanonicalName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String datasetName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_DATASET_NAME_FIELD_INDEX)).getStringValue();
        String indexName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_INDEX_NAME_FIELD_INDEX)).getStringValue();
        String fieldName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_FIELD_NAME_FIELD_INDEX)).getStringValue();
        boolean isAntimatter = ((ABoolean) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_ISANTIMATTER_FIELD_INDEX)).getBoolean();
        String nodeName =
                ((AString) statisticsRecord.getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_NODE_FIELD_INDEX))
                        .getStringValue();
        String partitionName =
                ((AString) statisticsRecord.getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_PARTITION_FIELD_INDEX))
                        .getStringValue();
        Long componentMinId = ((AInt64) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_COMPONENT_MIN_TIMESTAMP_INDEX)).getLongValue();
        Long componentMaxId = ((AInt64) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_COMPONENT_MAX_TIMESTAMP_INDEX)).getLongValue();
        ARecord synopsisRecord =
                (ARecord) statisticsRecord.getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_FIELD_INDEX);
        SynopsisType synopsisType = SynopsisType.valueOf(((AString) synopsisRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_TYPE_FIELD_INDEX)).getStringValue());
        int synopsisSize = ((AInt32) synopsisRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_SIZE_FIELD_INDEX)).getIntegerValue();
        AOrderedList elementsList = (AOrderedList) synopsisRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_ELEMENTS_FIELD_INDEX);
        IACursor cursor = elementsList.getCursor();
        List<ISynopsisElement> elems = new ArrayList<>(elementsList.size());
        Dataset ds;
        Datatype type = null;
        try {
            ds = metadataNode.getDataset(txnId, dataverseName, datasetName);

            type = metadataNode.getDatatype(txnId, ds.getItemTypeDataverseName(), ds.getItemTypeName());
        } catch (AlgebricksException e1) {
            e1.printStackTrace();
        }
        ITypeTraits keyTypeTraits =
                TypeTraitProvider.INSTANCE.getTypeTrait(((ARecordType) type.getDatatype()).getFieldType(fieldName));
        while (cursor.next()) {
            ARecord coeff = (ARecord) cursor.get();
            long uniqueValNum = 0l;
            int uniqueValNumPos = coeff.getType().getFieldIndex(
                    MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_UNIQUE_VALUES_NUM_FIELD_NAME);
            if (uniqueValNumPos >= 0) {
                uniqueValNum = ((AInt64) coeff.getValueByPos(uniqueValNumPos)).getLongValue();
            }
            try {
                elems.add(SynopsisElementFactory.createSynopsisElement(synopsisType,
                        ((AInt64) coeff
                                .getValueByPos(MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_KEY_FIELD_INDEX))
                                        .getLongValue(),
                        ((ADouble) coeff.getValueByPos(
                                MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_VALUE_FIELD_INDEX))
                                        .getDoubleValue(),
                        uniqueValNum, keyTypeTraits));
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }

        }
        AbstractSynopsis synopsis = null;
        try {
            synopsis = SynopsisFactory.createSynopsis(synopsisType, keyTypeTraits, elems, elems.size(), synopsisSize);
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

        return new Statistics(dataverseName.getCanonicalForm(), datasetName, indexName, fieldName, nodeName,
                partitionName, new ComponentStatisticsId(componentMinId, componentMaxId), false, isAntimatter,
                synopsis);

    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Statistics metadataEntity)
            throws HyracksDataException, MetadataException {
        IARecordBuilder synopsisRecordBuilder = new RecordBuilder();
        synopsisRecordBuilder.reset(MetadataRecordTypes.STATISTICS_SYNOPSIS_RECORDTYPE);

        // write the key in the first 8 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(metadataEntity.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getIndexName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getFieldName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        booleanSerde.serialize(metadataEntity.isAntimatter() ? ABoolean.TRUE : ABoolean.FALSE,
                tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getNode());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getPartition());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aInt64.setValue(metadataEntity.getComponentID().getMinTimestamp());
        int64Serde.serialize(aInt64, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the 9th field of the tuple
        recordBuilder.reset(MetadataRecordTypes.STATISTICS_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(metadataEntity.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(metadataEntity.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_DATASET_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(metadataEntity.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_INDEX_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(metadataEntity.getFieldName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_FIELD_NAME_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        booleanSerde.serialize(metadataEntity.isAntimatter() ? ABoolean.TRUE : ABoolean.FALSE,
                fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_ISANTIMATTER_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(metadataEntity.getNode());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_NODE_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(metadataEntity.getPartition());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_PARTITION_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        aInt64.setValue(metadataEntity.getComponentID().getMinTimestamp());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_COMPONENT_MIN_TIMESTAMP_INDEX, fieldValue);

        // write field 8
        fieldValue.reset();
        aInt64.setValue(metadataEntity.getComponentID().getMaxTimestamp());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_COMPONENT_MAX_TIMESTAMP_INDEX, fieldValue);

        // write field 9
        fieldValue.reset();
        writeSynopsisRecordType(synopsisRecordBuilder, metadataEntity.getSynopsis(), fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_FIELD_INDEX, fieldValue);

        // write record
        try {
            recordBuilder.write(tupleBuilder.getDataOutput(), true);
        } catch (HyracksDataException e) {
            throw new MetadataException(e);
        }
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeSynopsisRecordType(IARecordBuilder synopsisRecordBuilder,
            ISynopsis<? extends ISynopsisElement<Long>> synopsis, DataOutput dataOutput) throws HyracksDataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        IARecordBuilder synopsisElementRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();

        // write field 0
        fieldValue.reset();
        aString.setValue(synopsis.getType().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        synopsisRecordBuilder.addField(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_TYPE_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aInt32.setValue(synopsis.getSize());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        synopsisRecordBuilder.addField(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_SIZE_FIELD_INDEX, fieldValue);

        listBuilder.reset((AOrderedListType) MetadataRecordTypes.STATISTICS_SYNOPSIS_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_ELEMENTS_FIELD_INDEX]);
        for (ISynopsisElement<Long> synopsisElement : synopsis.getElements()) {
            // Skip synopsis elements with 0 value
            if (synopsisElement.getValue() != 0.0) {
                synopsisElementRecordBuilder.reset(MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_RECORDTYPE);
                itemValue.reset();

                // write subrecord field 0
                fieldValue.reset();
                aInt64.setValue(synopsisElement.getKey());
                int64Serde.serialize(aInt64, fieldValue.getDataOutput());
                synopsisElementRecordBuilder
                        .addField(MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_KEY_FIELD_INDEX, fieldValue);

                // write subrecord field 1
                fieldValue.reset();
                aDouble.setValue(synopsisElement.getValue());
                doubleSerde.serialize(aDouble, fieldValue.getDataOutput());
                synopsisElementRecordBuilder.addField(
                        MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_VALUE_FIELD_INDEX, fieldValue);

                // write optional field 2
                if (synopsisElement instanceof UniformHistogramBucket) {
                    ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();

                    fieldValue.reset();
                    nameValue.reset();
                    aString.setValue(
                            MetadataRecordTypes.STATISTICS_SYNOPSIS_ELEMENT_ARECORD_UNIQUE_VALUES_NUM_FIELD_NAME);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    aInt64.setValue(((UniformHistogramBucket) synopsisElement).getUniqueElementsNum());
                    int64Serde.serialize(aInt64, fieldValue.getDataOutput());
                    synopsisElementRecordBuilder.addField(nameValue, fieldValue);
                }

                synopsisElementRecordBuilder.write(itemValue.getDataOutput(), true);
                listBuilder.addItem(itemValue);
            }
        }
        // write field 2
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        synopsisRecordBuilder.addField(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_ELEMENTS_FIELD_INDEX,
                fieldValue);

        synopsisRecordBuilder.write(dataOutput, true);
    }

}
