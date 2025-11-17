/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBIngestionMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBIngestionMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBIngestionMetrics();
    }

    @Override
    public void tearDown() throws Exception {
        metrics.cleanup();
        super.tearDown();
    }

    public void testInitialize() {
        Counter samplesCounter = mock(Counter.class);
        Counter seriesCounter = mock(Counter.class);
        Counter chunksCounter = mock(Counter.class);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL),
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesCounter);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL),
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(seriesCounter);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL),
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksCounter);

        metrics.initialize(registry);

        assertSame(samplesCounter, metrics.samplesIngested);
        assertSame(seriesCounter, metrics.seriesCreated);
        assertSame(chunksCounter, metrics.memChunksCreated);

        verify(registry).createCounter(
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL,
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        verify(registry).createCounter(
            TSDBMetricsConstants.SERIES_CREATED_TOTAL,
            TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        verify(registry).createCounter(
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    public void testCleanup() {
        Counter samplesCounter = mock(Counter.class);
        Counter seriesCounter = mock(Counter.class);
        Counter chunksCounter = mock(Counter.class);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL),
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesCounter);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL),
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(seriesCounter);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL),
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksCounter);

        metrics.initialize(registry);
        assertNotNull(metrics.samplesIngested);
        assertNotNull(metrics.seriesCreated);
        assertNotNull(metrics.memChunksCreated);

        metrics.cleanup();

        assertNull(metrics.samplesIngested);
        assertNull(metrics.seriesCreated);
        assertNull(metrics.memChunksCreated);
    }

    public void testCleanupBeforeInitialization() {
        // Should not throw
        metrics.cleanup();

        assertNull(metrics.samplesIngested);
        assertNull(metrics.seriesCreated);
        assertNull(metrics.memChunksCreated);
    }

    public void testMultipleCleanupCalls() {
        Counter samplesCounter = mock(Counter.class);
        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL),
                eq(TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesCounter);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL),
                eq(TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL),
                eq(TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        metrics.initialize(registry);

        metrics.cleanup();
        metrics.cleanup(); // Should not throw

        assertNull(metrics.samplesIngested);
    }
}
