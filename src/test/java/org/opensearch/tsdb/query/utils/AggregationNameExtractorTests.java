/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.UnionStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for AggregationNameExtractor
 */
public class AggregationNameExtractorTests extends OpenSearchTestCase {

    public void testGetFinalAggregationName_pipelineAggregationPriority() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Create unfold aggregation (always present in TSDB aggregations)
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_agg",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfoldAgg);

        // Create coordinator pipeline aggregation
        TimeSeriesCoordinatorAggregationBuilder coordinatorAgg = new TimeSeriesCoordinatorAggregationBuilder(
            "coordinator_agg",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(),
            Map.of("input", "unfold_agg"),
            "input"
        );
        searchSourceBuilder.aggregation(coordinatorAgg);

        String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);
        assertEquals("Pipeline aggregation should be prioritized", "coordinator_agg", finalAggName);
    }

    public void testGetFinalAggregationName_withMultiplePipeline() {
        SearchSourceBuilder multiplePipelineAgg = new SearchSourceBuilder();

        // Create base unfold aggregation (always present in TSDB aggregations)
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "base_unfold",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );

        // Create first pipeline aggregation (scales by 2)
        TimeSeriesCoordinatorAggregationBuilder firstPipeline = new TimeSeriesCoordinatorAggregationBuilder(
            "first_pipeline",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(),
            Map.of("input", "base_unfold"),
            "input"
        );

        // Create second pipeline aggregation (scales by 3)
        TimeSeriesCoordinatorAggregationBuilder secondPipeline = new TimeSeriesCoordinatorAggregationBuilder(
            "second_pipeline",
            List.of(new ScaleStage(3.0)),
            new LinkedHashMap<>(),
            Map.of("input", "first_pipeline"),
            "input"
        );

        // Create final pipeline aggregation (scales by 4)
        TimeSeriesCoordinatorAggregationBuilder finalPipeline = new TimeSeriesCoordinatorAggregationBuilder(
            "final_pipeline",
            List.of(new ScaleStage(4.0)),
            new LinkedHashMap<>(),
            Map.of("input", "second_pipeline"),
            "input"
        );

        // Jumble the order of the aggregations
        multiplePipelineAgg.aggregation(finalPipeline);
        multiplePipelineAgg.aggregation(secondPipeline);
        multiplePipelineAgg.aggregation(firstPipeline);
        multiplePipelineAgg.aggregation(unfoldAgg);

        String result2 = AggregationNameExtractor.getFinalAggregationName(multiplePipelineAgg);
        assertEquals("Should return the last pipeline aggregation", "final_pipeline", result2);
    }

    public void testGetFinalAggregationName_multipleUnfoldsWithUnion() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Create multiple unfold aggregations
        TimeSeriesUnfoldAggregationBuilder firstUnfold = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_metric_a",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(firstUnfold);

        TimeSeriesUnfoldAggregationBuilder secondUnfold = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_metric_b",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(secondUnfold);

        TimeSeriesUnfoldAggregationBuilder thirdUnfold = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_metric_c",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(thirdUnfold);

        TimeSeriesCoordinatorAggregationBuilder unionCoordinator = new TimeSeriesCoordinatorAggregationBuilder(
            "union_coordinator",
            List.of(new UnionStage("unfold_metric_b"), new UnionStage("unfold_metric_c")),
            new LinkedHashMap<>(),
            Map.of("a", "unfold_metric_a", "b", "unfold_metric_b", "c", "unfold_metric_c"),
            "a"
        );
        searchSourceBuilder.aggregation(unionCoordinator);

        String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);
        assertEquals("Should return the union coordinator as final aggregation", "union_coordinator", finalAggName);
    }

    public void testGetFinalAggregationName_singleUnfoldWithoutCoordinator() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Edge case: Single unfold aggregation without coordinator (simple query)
        TimeSeriesUnfoldAggregationBuilder singleUnfold = new TimeSeriesUnfoldAggregationBuilder(
            "single_unfold",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(singleUnfold);

        String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);
        assertEquals("Should return the single unfold when no coordinator exists", "single_unfold", finalAggName);
    }

    public void testGetFinalAggregationName_nullSearchSourceBuilder() {
        // Following team convention: let NPE happen naturally rather than explicit null checks
        assertThrows(NullPointerException.class, () -> AggregationNameExtractor.getFinalAggregationName(null));
    }

    public void testGetFinalAggregationName_multipleUnreferencedAggregations() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Create two separate unfold aggregations
        TimeSeriesUnfoldAggregationBuilder unfold1 = new TimeSeriesUnfoldAggregationBuilder(
            "unfold1",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfold1);

        TimeSeriesUnfoldAggregationBuilder unfold2 = new TimeSeriesUnfoldAggregationBuilder(
            "unfold2",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfold2);

        // Create two DISCONNECTED pipeline chains - each references a different unfold
        TimeSeriesCoordinatorAggregationBuilder coord1 = new TimeSeriesCoordinatorAggregationBuilder(
            "coord1",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(),
            Map.of("input", "unfold1"),
            "input"
        );
        searchSourceBuilder.aggregation(coord1);

        TimeSeriesCoordinatorAggregationBuilder coord2 = new TimeSeriesCoordinatorAggregationBuilder(
            "coord2",
            List.of(new ScaleStage(3.0)),
            new LinkedHashMap<>(),
            Map.of("input", "unfold2"),
            "input"
        );
        searchSourceBuilder.aggregation(coord2);

        // Both coord1 and coord2 are unreferenced - this is invalid!
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder)
        );
        assertTrue("Exception should mention multiple unreferenced aggregations", exception.getMessage().contains("multiple unreferenced"));
        assertTrue("Exception should mention coord1", exception.getMessage().contains("coord1"));
        assertTrue("Exception should mention coord2", exception.getMessage().contains("coord2"));
    }

    public void testGetFinalAggregationName_circularDependency() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Create unfold aggregation
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_agg",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfoldAgg);

        // Create coord1 that references coord2 (not yet created)
        TimeSeriesCoordinatorAggregationBuilder coord1 = new TimeSeriesCoordinatorAggregationBuilder(
            "coord1",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(),
            Map.of("input", "coord2"),  // References coord2
            "input"
        );
        searchSourceBuilder.aggregation(coord1);

        // Create coord2 that references coord1 - circular dependency!
        TimeSeriesCoordinatorAggregationBuilder coord2 = new TimeSeriesCoordinatorAggregationBuilder(
            "coord2",
            List.of(new ScaleStage(3.0)),
            new LinkedHashMap<>(),
            Map.of("input", "coord1"),  // References coord1
            "input"
        );
        searchSourceBuilder.aggregation(coord2);

        // Both are referenced by each other - no unreferenced aggregation!
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder)
        );
        assertTrue("Exception should mention circular dependency", exception.getMessage().contains("circular dependency"));
    }

    public void testGetFinalAggregationName_multipleUnfoldsWithoutCoordinator() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Create multiple unfold aggregations WITHOUT a coordinator - this is invalid!
        TimeSeriesUnfoldAggregationBuilder unfold1 = new TimeSeriesUnfoldAggregationBuilder(
            "unfold1",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfold1);

        TimeSeriesUnfoldAggregationBuilder unfold2 = new TimeSeriesUnfoldAggregationBuilder(
            "unfold2",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );
        searchSourceBuilder.aggregation(unfold2);

        // Multiple unfolds without a coordinator - final aggregation cannot be determined, Invalid case!
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder)
        );
        assertTrue("Exception should mention expected count", exception.getMessage().contains("Expected exactly one unfold aggregation."));
    }

}
