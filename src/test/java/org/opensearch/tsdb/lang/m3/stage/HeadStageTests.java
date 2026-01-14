/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeadStageTests extends AbstractWireSerializingTestCase<HeadStage> {

    public void testConstructorWithLimit() {
        HeadStage stage = new HeadStage(5);
        assertEquals(5, stage.getLimit());
        assertEquals("head", stage.getName());
    }

    public void testConstructorWithDefaultLimit() {
        HeadStage stage = new HeadStage();
        assertEquals(10, stage.getLimit());
    }

    public void testConstructorWithNegativeLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new HeadStage(-1));
        assertEquals("Limit must be positive, got: -1", e.getMessage());
    }

    public void testConstructorWithZeroLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new HeadStage(0));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testProcessWithEmptyInput() {
        HeadStage stage = new HeadStage(5);
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        HeadStage stage = new HeadStage(5);
        TestUtils.assertNullInputThrowsException(stage, "head");
    }

    public void testFromArgsWithLimit() {
        Map<String, Object> args = Map.of("limit", 5);
        HeadStage stage = HeadStage.fromArgs(args);
        assertEquals(5, stage.getLimit());
    }

    public void testFromArgsWithDefaultLimit() {
        HeadStage stage = HeadStage.fromArgs(Map.of());
        assertEquals(10, stage.getLimit());
    }

    public void testFromArgsWithNullLimit() {
        Map<String, Object> args = new HashMap<>();
        args.put("limit", null);
        HeadStage stage = HeadStage.fromArgs(args);
        assertEquals(10, stage.getLimit());
    }

    public void testFromArgsWithInvalidStringLimit() {
        Map<String, Object> args = Map.of("limit", "invalid");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HeadStage.fromArgs(args));
        assertEquals("Invalid type for 'limit' argument. Expected integer, but got: invalid", e.getMessage());
    }

    public void testFromArgsWithZeroLimit() {
        Map<String, Object> args = Map.of("limit", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HeadStage.fromArgs(args));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testIsCoordinatorOnly() {
        HeadStage stage = new HeadStage(5);
        assertFalse(stage.isCoordinatorOnly());
    }

    public void testIsGlobalAggregation() {
        HeadStage stage = new HeadStage(5);
        assertTrue(stage.isGlobalAggregation());
    }

    public void testReduce() {
        HeadStage stage = new HeadStage(3);

        // Create mock aggregations with time series
        List<TimeSeries> series1 = List.of(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)),
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0))
        );
        List<TimeSeries> series2 = List.of(
            StageTestUtils.createTimeSeries("ts3", Map.of("name", "series3"), List.of(3.0)),
            StageTestUtils.createTimeSeries("ts4", Map.of("name", "series4"), List.of(4.0))
        );

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", series1, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", series2, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = List.of(agg1, agg2);

        InternalAggregation result = stage.reduce(aggregations, true);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 3 series (the limit)
        assertEquals(3, resultSeries.size());
        // Should be the first 3 series from all aggregations combined
        assertEquals("series1", resultSeries.get(0).getLabels().get("name"));
        assertEquals("series2", resultSeries.get(1).getLabels().get("name"));
        assertEquals("series3", resultSeries.get(2).getLabels().get("name"));
    }

    public void testReduceWithLimitGreaterThanTotal() {
        HeadStage stage = new HeadStage(10);

        List<TimeSeries> series1 = List.of(StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)));
        List<TimeSeries> series2 = List.of(StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0)));

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", series1, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", series2, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = List.of(agg1, agg2);

        InternalAggregation result = stage.reduce(aggregations, true);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 2 series (less than limit)
        assertEquals(2, resultSeries.size());
    }

    public void testReduceWithEmptyAggregations() {
        HeadStage stage = new HeadStage(5);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stage.reduce(Collections.emptyList(), true));
        assertEquals("Aggregations list cannot be null or empty", e.getMessage());
    }

    @Override
    protected HeadStage createTestInstance() {
        return new HeadStage(randomIntBetween(1, 100));
    }

    @Override
    protected Writeable.Reader<HeadStage> instanceReader() {
        return HeadStage::readFrom;
    }

    @Override
    protected HeadStage mutateInstance(HeadStage instance) {
        return new HeadStage(instance.getLimit() + 1);
    }
}
