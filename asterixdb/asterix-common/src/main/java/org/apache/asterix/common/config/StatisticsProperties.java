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
package org.apache.asterix.common.config;

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;
import static org.apache.hyracks.control.common.config.OptionTypes.DOUBLE;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;

import org.apache.asterix.common.statistics.SynopsisMergeStrategy;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;

public class StatisticsProperties extends AbstractProperties {

    public enum Option implements IOption {

        STATISTICS_PRIMARY_KEYS_ENABLED(BOOLEAN, false, "Whether to keep statistics on primary keys or not"),
        STATISTICS_SYNOPSIS_TYPE(STRING, "None", "The type of synopsis used for collecting dataset statistics"),
        STATISTICS_SYNOPSIS_SIZE(
                INTEGER,
                100,
                "The size of synopsis (number of histogram buckets or coefficients in wavelet transform)."),
        STATISTICS_MERGE_STRATEGY(STRING, "TimeBased", "Statistics merge strategy (if applicable)"),
        STATISTICS_MERGE_STRATEGY_TIMEOUT(
                INTEGER,
                -1,
                "The timeout in milliseconds between the statistics merge operation invocations"),
        STATISTICS_SKETCH_FANOUT(
                INTEGER,
                2,
                "Fanout of the search tree for GroupCount sketch. Determines the tradeoff "
                        + "between sketch query and update time."),
        STATISTICS_SKETCH_FAILURE_PROBABILITY(
                DOUBLE,
                0.01,
                "Probability that sketched result will not be within " + "accuracy bounds"),
        STATISTICS_SKETCH_ACCURACY(
                DOUBLE,
                0.1,
                "Parameter specifying that sketched result is within a factor of " + "(1±accuracy) of the true result"),
        STATISTICS_SKETCH_ENERGY_ACCURACY(
                DOUBLE,
                0.1,
                "Parameter specifying that sketched synopsis has energy (sum of squares) at least "
                        + "(1±accuracy)*energy_accuracy of the total result energy");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    public static String STATISTICS_SYNOPSIS_TYPE_KEY = Option.STATISTICS_SYNOPSIS_TYPE.ini();

    public static String STATISTICS_SYNOPSIS_SIZE_KEY = Option.STATISTICS_SYNOPSIS_SIZE.ini();

    public static String STATISTICS_PRIMARY_KEYS_ENABLED = Option.STATISTICS_PRIMARY_KEYS_ENABLED.ini();

    public StatisticsProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public boolean isStatisticsOnPrimaryKeysEnabled() {
        return accessor.getBoolean(Option.STATISTICS_PRIMARY_KEYS_ENABLED);
    }

    public SynopsisType getStatisticsSynopsisType() {
        return SynopsisType.valueOf(accessor.getString(Option.STATISTICS_SYNOPSIS_TYPE));
    }

    public int getStatisticsSize() {
        return accessor.getInt(Option.STATISTICS_SYNOPSIS_SIZE);
    }

    public SynopsisMergeStrategy getStatisticsMergeStrategy() {
        return SynopsisMergeStrategy.valueOf(accessor.getString(Option.STATISTICS_MERGE_STRATEGY));
    }

    public int getStatisticsMergeTimeout() {
        return accessor.getInt(Option.STATISTICS_MERGE_STRATEGY_TIMEOUT);
    }

    public int getSketchFanout() {
        return accessor.getInt(Option.STATISTICS_SKETCH_FANOUT);
    }

    public double getSketchAccuracy() {
        return accessor.getDouble(Option.STATISTICS_SKETCH_ACCURACY);
    }

    public double getSketchEnergyAccuracy() {
        return accessor.getDouble(Option.STATISTICS_SKETCH_ENERGY_ACCURACY);
    }

    public double getSketchFailureProbability() {
        return accessor.getDouble(Option.STATISTICS_SKETCH_FAILURE_PROBABILITY);
    }
}
