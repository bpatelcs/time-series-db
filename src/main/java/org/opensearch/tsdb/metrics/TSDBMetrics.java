/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;

/** TSDB metrics: counters and histograms initialized once via telemetry. */
public class TSDBMetrics {
    private static final Logger logger = LogManager.getLogger(TSDBMetrics.class);
    private static volatile MetricsRegistry registry;

    public static final TSDBIngestionMetrics INGESTION = new TSDBIngestionMetrics();
    public static final TSDBAggregationMetrics AGGREGATION = new TSDBAggregationMetrics();

    // Public constructor for testing
    public TSDBMetrics() {}

    /**
     * Initialize all TSDB metrics. Safe to call once; subsequent calls are ignored.
     */
    public static synchronized void initialize(MetricsRegistry metricsRegistry) {
        if (metricsRegistry == null) {
            throw new IllegalArgumentException("MetricsRegistry cannot be null");
        }
        // Skip initialization if a Noop registry is provided.
        if (isNoopRegistry(metricsRegistry)) {
            logger.warn("Noop MetricsRegistry provided; skipping TSDB metrics initialization");
            return;
        }
        if (registry != null) {
            logger.warn("TSDBMetrics already initialized, skipping re-initialization");
            return;
        }

        // Initialize metrics first (may throw exception)
        INGESTION.initialize(metricsRegistry);
        AGGREGATION.initialize(metricsRegistry);

        // Only set registry after successful initialization
        registry = metricsRegistry;
    }

    /**
     * Check if metrics have been initialized.
     */
    public static boolean isInitialized() {
        return registry != null;
    }

    /**
     * Get the underlying MetricsRegistry.
     */
    public static MetricsRegistry getRegistry() {
        return registry;
    }

    /**
     * Safely increment a counter by a specific amount.
     * Provides null safety and initialization checks.
     */
    public static void incrementCounter(Counter counter, long value) {
        if (isInitialized() && counter != null) {
            counter.add(value);
        }
    }

    /**
     * Safely record a histogram value.
     * Provides null safety and initialization checks.
     */
    public static void recordHistogram(Histogram histogram, double value) {
        if (isInitialized() && histogram != null) {
            histogram.record(value);
        }
    }

    private static boolean isNoopRegistry(MetricsRegistry r) {
        try {
            String name = r.getClass().getName();
            if (name != null && name.toLowerCase(java.util.Locale.ROOT).contains("noop")) {
                return true;
            }
            String desc = r.toString();
            return desc != null && desc.toLowerCase(java.util.Locale.ROOT).contains("noop");
        } catch (Exception e) {
            return false;
        }
    }

    /** Cleanup all metrics (for tests). */
    public static synchronized void cleanup() {
        registry = null;
        INGESTION.cleanup();
        AGGREGATION.cleanup();
        logger.info("TSDB metrics cleanup completed");
    }
}
