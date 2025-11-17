/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        when(registry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(registry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
    }

    @Override
    public void tearDown() throws Exception {
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    public void testInitializeSuccess() {
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        TSDBMetrics.initialize(registry);

        assertTrue(TSDBMetrics.isInitialized());
        assertSame(registry, TSDBMetrics.getRegistry());
        assertNotNull(TSDBMetrics.INGESTION.samplesIngested);
        assertNotNull(TSDBMetrics.INGESTION.seriesCreated);
        assertNotNull(TSDBMetrics.INGESTION.memChunksCreated);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.postCollectLatency);
    }

    public void testInitializeWithNullRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        Exception e = expectThrows(IllegalArgumentException.class, () -> { TSDBMetrics.initialize(null); });
        assertEquals("MetricsRegistry cannot be null", e.getMessage());
        assertFalse(TSDBMetrics.isInitialized());
    }

    public void testInitializeSkippedForNoopRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry noop = mock(MetricsRegistry.class);
        when(noop.toString()).thenReturn("org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry");

        TSDBMetrics.initialize(noop);

        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
        assertNull(TSDBMetrics.INGESTION.samplesIngested);
        assertNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testInitializeAfterNoopRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry noop = mock(MetricsRegistry.class);
        when(noop.toString()).thenReturn("org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry");

        TSDBMetrics.initialize(noop); // should be skipped
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        // Now initialize with real registry
        TSDBMetrics.initialize(registry);
        assertTrue(TSDBMetrics.isInitialized());
        assertSame(registry, TSDBMetrics.getRegistry());
        assertNotNull(TSDBMetrics.INGESTION.samplesIngested);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testDoubleInitialization() {
        TSDBMetrics.initialize(registry);
        Counter firstCounter = TSDBMetrics.INGESTION.samplesIngested;

        // Second initialization should be ignored
        MetricsRegistry newRegistry = mock(MetricsRegistry.class);
        TSDBMetrics.initialize(newRegistry);

        assertSame(firstCounter, TSDBMetrics.INGESTION.samplesIngested);
        assertSame(registry, TSDBMetrics.getRegistry());
    }

    public void testInitializeFailureRollback() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry badRegistry = mock(MetricsRegistry.class);
        when(badRegistry.createCounter(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Registry error"));

        expectThrows(RuntimeException.class, () -> { TSDBMetrics.initialize(badRegistry); });

        // Should rollback on failure
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
    }

    public void testCleanup() {
        TSDBMetrics.initialize(registry);
        assertTrue(TSDBMetrics.isInitialized());
        assertNotNull(TSDBMetrics.INGESTION.samplesIngested);

        TSDBMetrics.cleanup();

        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
        assertNull(TSDBMetrics.INGESTION.samplesIngested);
        assertNull(TSDBMetrics.INGESTION.seriesCreated);
        assertNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testCleanupBeforeInitialization() {
        // Should not throw
        TSDBMetrics.cleanup();
        assertFalse(TSDBMetrics.isInitialized());
    }

    public void testIncrementCounterWhenInitialized() {
        TSDBMetrics.initialize(registry);
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.incrementCounter(mockCounter, 5);
        verify(mockCounter).add(5);

        TSDBMetrics.incrementCounter(mockCounter, 10);
        verify(mockCounter).add(10);
    }

    public void testIncrementCounterWhenNotInitialized() {
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.incrementCounter(mockCounter, 5);

        verify(mockCounter, never()).add(5);
    }

    public void testIncrementCounterWithNullCounter() {
        TSDBMetrics.initialize(registry);

        // Should not throw
        TSDBMetrics.incrementCounter(null, 5);
    }

    public void testIncrementCounterAfterCleanup() {
        TSDBMetrics.initialize(registry);
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.cleanup();
        TSDBMetrics.incrementCounter(mockCounter, 5);

        verify(mockCounter, never()).add(5);
    }

    public void testRecordHistogramWhenInitialized() {
        TSDBMetrics.initialize(registry);
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.recordHistogram(mockHistogram, 100.5);
        verify(mockHistogram).record(100.5);

        TSDBMetrics.recordHistogram(mockHistogram, 50.25);
        verify(mockHistogram).record(50.25);
    }

    public void testRecordHistogramWhenNotInitialized() {
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.recordHistogram(mockHistogram, 100.0);

        verify(mockHistogram, never()).record(100.0);
    }

    public void testRecordHistogramWithNullHistogram() {
        TSDBMetrics.initialize(registry);

        // Should not throw
        TSDBMetrics.recordHistogram(null, 100.0);
    }

    public void testRecordHistogramAfterCleanup() {
        TSDBMetrics.initialize(registry);
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.cleanup();
        TSDBMetrics.recordHistogram(mockHistogram, 100.0);

        verify(mockHistogram, never()).record(100.0);
    }

    public void testIngestionMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.INGESTION);
        assertNotNull(TSDBMetrics.INGESTION.samplesIngested);
        assertNotNull(TSDBMetrics.INGESTION.seriesCreated);
        assertNotNull(TSDBMetrics.INGESTION.memChunksCreated);
    }

    public void testAggregationMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.AGGREGATION);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.postCollectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.docsTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.docsLive);
        assertNotNull(TSDBMetrics.AGGREGATION.docsClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksLive);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesLive);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksForDocErrors);
    }

    public void testMetricsRegistryAccessors() {
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        TSDBMetrics.initialize(registry);

        assertTrue(TSDBMetrics.isInitialized());
        assertNotNull(TSDBMetrics.getRegistry());
        assertEquals(registry, TSDBMetrics.getRegistry());
    }

    public void testConstructorForTestingCoverage() {
        // Public constructor exists for testing
        TSDBMetrics metrics = new TSDBMetrics();
        assertNotNull(metrics);
    }

    public void testMultipleCleanupCalls() {
        TSDBMetrics.initialize(registry);

        TSDBMetrics.cleanup();
        TSDBMetrics.cleanup(); // Should not throw

        assertFalse(TSDBMetrics.isInitialized());
    }
}
