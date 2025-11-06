/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Input data configuration for time series testing
 *
 * Supports two modes:
 *
 * 1. FIXED INTERVAL DATA (input_data_type: FIXED_INTERVAL with time_config and metrics):
 *    - Uses fixed time intervals defined by min_timestamp, max_timestamp, and step
 *    - Values are provided as an array that aligns with generated timestamps
 *    - Null values in the array represent missing data points (blips) at those timestamps
 *
 * 2. GENERIC DATA (input_data_type: GENERIC with generic_metrics):
 *    - Each data point has an explicit timestamp-value pair
 *    - Missing data is represented by absence of data points (no entry for that time)
 *    - No null values needed - if there's no data at a time, simply don't include that timestamp
 *
 * The indexName field specifies which index this data should be ingested into.
 * It must match one of the index names defined in the test setup's index_configs.
 */
public record InputDataConfig(@JsonProperty("index_name") String indexName, @JsonProperty("input_data_type") InputDataType inputDataType,
    @JsonProperty("time_config") TimeConfig timeConfig, @JsonProperty("regular_metrics") List<FixedIntervalMetricData> metrics,
    @JsonProperty("metrics") List<MetricData> genericMetrics) {
    /**
     * Check if this configuration is for fixed interval time series data
     */
    public boolean isFixedIntervalMode() {
        if (inputDataType != null) {
            return inputDataType == InputDataType.FIXED_INTERVAL;
        }
        // Auto-detect based on data presence
        boolean hasFixedIntervalData = timeConfig != null || (metrics != null && !metrics.isEmpty());
        boolean hasGenericData = genericMetrics != null && !genericMetrics.isEmpty();
        return hasFixedIntervalData && !hasGenericData;
    }

    /**
     * Check if this configuration is for generic time series data
     */
    public boolean isGenericMode() {
        if (inputDataType != null) {
            return inputDataType == InputDataType.GENERIC;
        }
        return !isFixedIntervalMode();
    }

    /**
     * Validate the configuration and determine the data mode
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() {
        boolean hasFixedIntervalData = timeConfig != null || (metrics != null && !metrics.isEmpty());
        boolean hasGenericData = genericMetrics != null && !genericMetrics.isEmpty();

        if (hasFixedIntervalData && hasGenericData) {
            throw new IllegalArgumentException(
                "Invalid configuration: Cannot specify both fixed interval (time_config/metrics) and generic (generic_metrics) data. "
                    + "Use input_data_type to explicitly specify the mode."
            );
        }

        if (inputDataType != null) {
            // Explicit mode specified - validate it matches the data
            if (inputDataType == InputDataType.FIXED_INTERVAL && hasGenericData) {
                throw new IllegalArgumentException(
                    "Invalid configuration: input_data_type is FIXED_INTERVAL but generic_metrics is provided"
                );
            }
            if (inputDataType == InputDataType.GENERIC && hasFixedIntervalData) {
                throw new IllegalArgumentException(
                    "Invalid configuration: input_data_type is GENERIC but only fixed interval data (time_config/metrics) is provided"
                );
            }
        }

        // Validate required fields based on mode
        if (inputDataType != null && inputDataType == InputDataType.FIXED_INTERVAL) {
            if (timeConfig == null) {
                throw new IllegalArgumentException("time_config is required for fixed interval data mode");
            }
            if (metrics == null || metrics.isEmpty()) {
                throw new IllegalArgumentException("metrics is required for fixed interval data mode");
            }
        } else if (inputDataType != null && inputDataType == InputDataType.GENERIC) {
            if (genericMetrics == null || genericMetrics.isEmpty()) {
                throw new IllegalArgumentException("generic_metrics is required for generic data mode");
            }
        } else {
            // Auto-detect mode - validate based on what data is present
            if (hasFixedIntervalData) {
                if (timeConfig == null) {
                    throw new IllegalArgumentException("time_config is required for fixed interval data mode");
                }
                if (metrics == null || metrics.isEmpty()) {
                    throw new IllegalArgumentException("metrics is required for fixed interval data mode");
                }
            } else if (hasGenericData) {
                if (genericMetrics == null || genericMetrics.isEmpty()) {
                    throw new IllegalArgumentException("generic_metrics is required for generic data mode");
                }
            } else {
                throw new IllegalArgumentException("No data configuration provided. Specify either fixed interval or generic data.");
            }
        }
    }

}
