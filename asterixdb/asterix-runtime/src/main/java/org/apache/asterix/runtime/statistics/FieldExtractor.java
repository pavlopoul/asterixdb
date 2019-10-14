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
package org.apache.asterix.runtime.statistics;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntegerSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;

public class FieldExtractor implements IFieldExtractor<Long> {
    private static final long serialVersionUID = 1L;

    private AIntegerSerializerDeserializer serde;
    private int fieldIdx;
    private final String fieldName;
    private final ITypeTraits fieldTypeTraits;
    private final ATypeTag typeTag;

    public FieldExtractor(AIntegerSerializerDeserializer serde, int fieldIdx, String fieldName,
            ITypeTraits fieldTypeTraits, ATypeTag typeTag) {
        this.serde = serde;
        this.fieldIdx = fieldIdx;
        this.fieldName = fieldName;
        this.fieldTypeTraits = fieldTypeTraits;
        this.typeTag = typeTag;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ITypeTraits getFieldTypeTraits() {
        return fieldTypeTraits;
    }

    @Override
    public boolean isUnordered() {
        return false;
    }

    @Override
    public Long extractFieldValue(ITupleReference tuple) throws HyracksDataException {
        byte[] data = tuple.getFieldData(fieldIdx);
        int startOffset = tuple.getFieldStart(fieldIdx);
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[startOffset]);
        if (tag != typeTag) {
            throw new HyracksDataException("Expected to see " + typeTag + ", but got " + tag);
        }
        return serde.getLongValue(data, startOffset + 1);
    }
}
