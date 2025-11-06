/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TimeConfig;
import org.opensearch.tsdb.framework.models.MetricData;
import org.opensearch.tsdb.framework.models.FixedIntervalMetricData;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TimestampUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates time series samples from test case input data configurations.
 *
 * Supports two modes:
 * 1. Fixed interval data: Generates samples with fixed timestamps based on time_config
 * 2. Generic data: Creates samples from explicit timestamp-value pairs
 */
public final class TimeSeriesSampleGenerator {

    // Private constructor to prevent instantiation
    private TimeSeriesSampleGenerator() {}

    /**
     * Generate time series samples with timestamps based on input data configuration
     *
     * Handles two modes:
     * 1. Fixed interval data: Fixed timestamps with step intervals. Null values in the array represent missing data.
     * 2. Generic data: Explicit timestamp-value pairs. Missing data is represented by absence of data points.
     */
    public static List<TimeSeriesSample> generateSamples(InputDataConfig inputDataConfig) {
        if (inputDataConfig == null) {
            return new ArrayList<>();
        }

        // Validate the configuration to catch any bad input
        inputDataConfig.validate();

        // Check if this is generic data mode
        if (!inputDataConfig.isFixedIntervalMode()) {
            return generateGenericSamples(inputDataConfig);
        }

        // Fixed interval data mode - requires time_config and metrics
        List<FixedIntervalMetricData> metrics = inputDataConfig.metrics();
        if (metrics == null || metrics.isEmpty()) {
            return new ArrayList<>();
        }

        // Get common time configuration
        TimeConfig timeConfig = inputDataConfig.timeConfig();
        if (timeConfig == null) {
            throw new IllegalArgumentException("time_config is required under input_data for fixed interval metrics");
        }

        // Generate common timestamps for all metrics
        List<Instant> timestamps = TimestampUtils.generateTimestampRange(
            timeConfig.minTimestamp(),
            timeConfig.maxTimestamp(),
            timeConfig.step()
        );

        // Pre-allocate with estimated capacity (timestamps * metrics, assuming most values are non-null)
        List<TimeSeriesSample> samples = new ArrayList<>(timestamps.size() * metrics.size());

        // Create samples for each metric
        for (FixedIntervalMetricData metric : metrics) {
            Double[] values = metric.values();
            int count = Math.min(timestamps.size(), values.length);
            var labels = metric.labels();

            for (int i = 0; i < count; i++) {
                // For fixed interval data: null values in the array represent missing data points (gaps/blips)
                // We skip these nulls during ingestion
                if (values[i] != null) {
                    samples.add(new TimeSeriesSample(timestamps.get(i), values[i], labels));
                }
            }
        }

        return samples;
    }

    /**
     * Generate samples for generic time series data
     * Note: For generic data, missing data points are simply not included.
     * There's no need for null values since we explicitly specify each timestamp-value pair.
     */
    private static List<TimeSeriesSample> generateGenericSamples(InputDataConfig inputDataConfig) {
        List<MetricData> genericMetrics = inputDataConfig.genericMetrics();
        if (genericMetrics == null || genericMetrics.isEmpty()) {
            return new ArrayList<>();
        }

        // Estimate capacity based on metrics count (rough estimate)
        List<TimeSeriesSample> samples = new ArrayList<>(genericMetrics.size() * 10);

        // Process each generic metric
        for (MetricData metric : genericMetrics) {
            List<MetricData.DataPoint> dataPoints = metric.dataPoints();
            if (dataPoints != null && !dataPoints.isEmpty()) {
                var labels = metric.labels();
                for (MetricData.DataPoint dataPoint : dataPoints) {
                    samples.add(new TimeSeriesSample(dataPoint.timestamp(), dataPoint.value(), labels));
                }
            }
        }

        return samples;
    }
}
