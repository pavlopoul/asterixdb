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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;

public abstract class AbstractStatisticsFactory implements IStatisticsFactory, Serializable {
    private static final long serialVersionUID = 1L;

    protected final List<IFieldExtractor> extractors;
    protected final SynopsisType type;

    protected AbstractStatisticsFactory(List<IFieldExtractor> extractors, SynopsisType type) {
        this.extractors = extractors;
        this.type = type;
    }

    @Override
    public ISynopsisBuilder createStatistics(ComponentStatistics componentStatistics, boolean isBulkload)
            throws HyracksDataException {
        List<ISynopsisBuilder> builders = new ArrayList<>();
        for (IFieldExtractor e : extractors) {
            if (e.isUnordered() && type.needsSortedOrder()) {
                continue;
            }
            ISynopsisBuilder synopsisBuilder = createSynopsisBuilder(componentStatistics, false, e);
            if (isBulkload) {
                builders.add(synopsisBuilder);
            } else {
                ISynopsisBuilder antimatterSynopsisBuilder = createSynopsisBuilder(componentStatistics, true, e);
                builders.add(new CombinedSynopsisBuilder(synopsisBuilder, antimatterSynopsisBuilder));
            }
        }
        if (builders.size() == 1) {
            return builders.get(0);
        } else {
            return new DelegatingSynopsisBuilder(builders);
        }
    }

    protected abstract ISynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, IFieldExtractor fieldExtractor) throws HyracksDataException;
}
