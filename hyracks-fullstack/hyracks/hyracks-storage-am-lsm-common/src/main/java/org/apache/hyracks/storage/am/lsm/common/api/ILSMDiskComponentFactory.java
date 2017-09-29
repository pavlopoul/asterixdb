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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;

@FunctionalInterface
public interface ILSMDiskComponentFactory {

    /**
     * Create a disk component from the file references
     *
     * @param cfr
     *            the disk file references which points to the different physical files of the index
     * @param numTuples
     *            number of tuples in a new disk component
     * @param numAntimatterTuples
     *            number of antimatter tuples in a new disk component
     * @return a disk component
     * @throws HyracksDataException
     */
    ILSMDiskComponent createComponent(AbstractLSMIndex lsmIndex, LSMComponentFileReferences cfr)
            throws HyracksDataException;
}
