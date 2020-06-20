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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public interface ISynopsisElement<T> extends Serializable {

    final static int SYNOPSIS_KEY_SIZE = Long.BYTES;
    final static int SYNOPSIS_VALUE_SIZE = Double.BYTES;

    @SuppressWarnings("rawtypes")
    final ISerializerDeserializer SYNOPSIS_KEY_SERDE = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    final ISerializerDeserializer SYNOPSIS_VALUE_SERDE = DoubleSerializerDeserializer.INSTANCE;

    T getKey();

    double getValue();

}
