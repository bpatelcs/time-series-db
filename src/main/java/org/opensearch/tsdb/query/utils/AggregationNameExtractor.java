/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;

import reactor.util.annotation.NonNull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for extracting aggregation names from OpenSearch SearchSourceBuilder objects.
 *
 * @since 0.0.1
 */
public final class AggregationNameExtractor {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private AggregationNameExtractor() {}

    /**
     * Extracts the final aggregation name from a SearchSourceBuilder.
     *
     * <p>This method analyzes the aggregation structure in the provided SearchSourceBuilder
     * and returns the name of the final aggregation. The method prioritizes pipeline
     * aggregations over regular aggregations, as pipeline aggregations are typically
     * the final step in aggregation processing.
     *
     * <p>If pipeline aggregations exist, the method identifies the final aggregation by
     * finding the aggregation that is not referenced by any other pipeline aggregation
     * (i.e., the leaf node in the dependency graph). This ensures correctness regardless
     * of the order in which aggregations were registered.
     *
     * <p>If no pipeline aggregations exist, it expects exactly one regular aggregation
     * (typically a {@code TimeSeriesUnfoldAggregationBuilder}) and returns its name.
     * If multiple regular aggregations exist without pipeline aggregations, an
     * {@code IllegalStateException} is thrown as this indicates an invalid aggregation structure.
     *
     * <p><strong>Precondition:</strong> In TSDB, there is always at least one
     * {@code TimeSeriesUnfoldAggregationBuilder} present in the SearchSourceBuilder.
     * This method assumes aggregations have been configured.
     *
     * @param searchSourceBuilder the SearchSourceBuilder containing the aggregations to analyze (must not be null)
     * @return the name of the final aggregation
     * @throws NullPointerException if searchSourceBuilder is null
     * @throws IllegalStateException if there are circular dependencies in pipeline aggregations,
     *         multiple disconnected pipeline chains, or multiple regular aggregations without pipeline aggregations
     */
    public static String getFinalAggregationName(@NonNull SearchSourceBuilder searchSourceBuilder) {
        Collection<PipelineAggregationBuilder> pipelineBuilders = searchSourceBuilder.aggregations().getPipelineAggregatorFactories();

        if (!pipelineBuilders.isEmpty()) {
            // Collect all aggregation names that are referenced by pipeline aggregations
            Set<String> referencedAggNames = new HashSet<>();
            for (PipelineAggregationBuilder builder : pipelineBuilders) {
                if (builder instanceof TimeSeriesCoordinatorAggregationBuilder coordBuilder) {
                    referencedAggNames.addAll(coordBuilder.getReferences().values());
                }
            }

            // Find the pipeline aggregation that is NOT referenced by any other aggregation
            // This is the leaf node in the dependency graph - the final aggregation
            Set<String> unreferencedAggNames = new HashSet<>();
            for (PipelineAggregationBuilder builder : pipelineBuilders) {
                if (!referencedAggNames.contains(builder.getName())) {
                    unreferencedAggNames.add(builder.getName());
                }
            }

            if (unreferencedAggNames.size() == 1) {
                // Exactly one final aggregation
                return unreferencedAggNames.iterator().next();
            } else if (unreferencedAggNames.isEmpty()) {
                // No unreferenced aggregations - circular dependency
                throw new IllegalStateException("There is a circular dependency in the pipeline aggregations");
            } else {
                // Multiple unreferenced aggregations - invalid structure with disconnected pipeline chains
                throw new IllegalStateException(
                    "Found multiple unreferenced pipeline aggregations: "
                        + unreferencedAggNames
                        + ". This indicates disconnected pipeline chains. There should be exactly one final aggregation."
                );
            }
        }

        // Fallback to regular aggregations if no pipeline aggregations exist
        // In TSDB, if there are no pipeline aggregations, there should be exactly one unfold aggregation
        Collection<AggregationBuilder> regularAggs = searchSourceBuilder.aggregations().getAggregatorFactories();

        if (regularAggs.size() == 1) {
            return regularAggs.iterator().next().getName();
        } else {
            throw new IllegalStateException(
                "Found "
                    + regularAggs.size()
                    + " regular aggregations without pipeline aggregations. Expected exactly one unfold aggregation."
            );
        }
    }
}
