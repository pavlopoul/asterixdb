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
package org.apache.asterix.formats.nontagged;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;

public class TypeTraitProvider implements ITypeTraitProvider {

    // WARNING: the byte sizes depend on the serializer!
    // currently assuming a serializer that adds a 1-byte type indicator before
    // the data
    private static final ITypeTraits ONEBYTETYPETRAIT = new FixedLengthTypeTrait(1 + 1);
    private static final ITypeTraits TWOBYTETYPETRAIT = new FixedLengthTypeTrait(2 + 1);
    private static final ITypeTraits FOURBYTETYPETRAIT = new FixedLengthTypeTrait(4 + 1);
    private static final ITypeTraits EIGHTBYTETYPETRAIT = new FixedLengthTypeTrait(8 + 1);
    private static final ITypeTraits SIXTEENBYTETYPETRAIT = new FixedLengthTypeTrait(16 + 1);
    private static final ITypeTraits SEVENTEENBYTETYPETRAIT = new FixedLengthTypeTrait(17 + 1);
    private static final ITypeTraits THIRTYTWOBYTETYPETRAIT = new FixedLengthTypeTrait(32 + 1);
    private static final ITypeTraits TWENTYFOURBYTETYPETRAIT = new FixedLengthTypeTrait(24 + 1);

    private static final ITypeTraits VARLENTYPETRAIT = VarLengthTypeTrait.INSTANCE;

    public static final TypeTraitProvider INSTANCE = new TypeTraitProvider();

    @Override
    public ITypeTraits getTypeTrait(Object typeInfo) {
        IAType type = (IAType) typeInfo;
        if (type == null) {
            return null;
        }
        switch (type.getTypeTag()) {
            case BOOLEAN:
            case TINYINT:
                return ONEBYTETYPETRAIT;
            case SMALLINT:
                return TWOBYTETYPETRAIT;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME:
                return FOURBYTETYPETRAIT;
            case BIGINT:
            case DOUBLE:
            case DATETIME:
                return EIGHTBYTETYPETRAIT;
            case POINT:
            case UUID:
                return SIXTEENBYTETYPETRAIT;
            case INTERVAL:
                return SEVENTEENBYTETYPETRAIT;
            case POINT3D:
                return TWENTYFOURBYTETYPETRAIT;
            case LINE:
                return THIRTYTWOBYTETYPETRAIT;
            default: {
                return VARLENTYPETRAIT;
            }
        }
    }
}