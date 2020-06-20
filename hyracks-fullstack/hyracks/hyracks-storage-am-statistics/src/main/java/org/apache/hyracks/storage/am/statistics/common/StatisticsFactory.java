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

import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.historgram.EquiHeightHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.sketch.quantile.QuantileSketchBuilder;

public class StatisticsFactory extends AbstractStatisticsFactory {
    private static final Logger LOGGER = Logger.getLogger(StatisticsFactory.class.getName());

    private final String dataverseName;
    private final String datasetName;
    private final String indexName;
    private final int size;
    private final double energyAccuracy;
    private final int fanout;
    private double failureProbability;
    private double accuracy;

    public StatisticsFactory(SynopsisType type, String dataverseName, String datasetName, String indexName,
            List<IFieldExtractor> extractors, int size, int fanout, double failureProbability, double accuracy,
            double energyAccuracy) {
        super(extractors, type);
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.size = size;
        this.fanout = fanout;
        this.failureProbability = failureProbability;
        this.accuracy = accuracy;
        this.energyAccuracy = energyAccuracy;
    }

    @Override
    public boolean canCollectStats() {
        for (IFieldExtractor extractor : extractors) {
            // for now support only fixed length fields
            if (!extractor.getFieldTypeTraits().isFixedLength()) {
                return false;
            }
            //check if the key can be mapped on long domain, i.e. key length  <= 8 bytes. 1 byte is reserved for typeTag
            if (extractor.getFieldTypeTraits().getFixedLength() > (Long.SIZE / 8 + 1)) {
                LOGGER.warning("Unable to collect statistics for keys with size greater than 8 bytes"
                        + extractor.getFieldTypeTraits());
                return false;
            }
        }
        return true;
    }

    protected AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, IFieldExtractor fieldExtractor) throws HyracksDataException {
        long numElements =
                isAntimatter ? componentStatistics.getNumAntimatterTuples() : componentStatistics.getNumTuples();
        ISynopsis synopsis = SynopsisFactory.createSynopsis(type, fieldExtractor.getFieldTypeTraits(),
                SynopsisElementFactory.createSynopsisElementsCollection(type, size), numElements, size);
        switch (type) {
            case QuantileSketch:
                return new QuantileSketchBuilder((EquiHeightHistogramSynopsis) synopsis, dataverseName, datasetName,
                        indexName, fieldExtractor.getFieldName(), isAntimatter, fieldExtractor, componentStatistics,
                        accuracy);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis builder for type " + type);
        }
    }
}
