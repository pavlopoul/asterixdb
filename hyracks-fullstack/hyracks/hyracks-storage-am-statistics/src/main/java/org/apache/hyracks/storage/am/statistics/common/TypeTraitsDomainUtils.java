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

package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;

// Class methods obtain numeric domain bounds by considering appropriate ITypeTraits assuming that numeric domain is signed
public class TypeTraitsDomainUtils {

    public static long minDomainValue(ITypeTraits typeTraits) throws HyracksDataException {
        return ~maxDomainValue(typeTraits);
    }

    public static long maxDomainValue(ITypeTraits typeTraits) throws HyracksDataException {
        if (!typeTraits.isFixedLength())
            throw new HyracksDataException("Cannot calculate domain for variable size type");
        // subtract 1 from fixedLength assuming that 1 byte is reserved for type tag
        return (1l << (((typeTraits.getFixedLength() - 1) * 8) - 1)) - 1;
    }

    public static int maxLevel(ITypeTraits typeTraits) throws HyracksDataException {
        if (!typeTraits.isFixedLength())
            throw new HyracksDataException("Cannot calculate domain for variable size type");
        // subtract 1 from fixedLength assuming that 1 byte is reserved for type tag
        return ((typeTraits.getFixedLength() - 1) * 8);
    }
}
