/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's head function.
 *
 * Returns the first n series from the series list.
 *
 * Usage: fetch a | head 5
 *        fetch a | head    (defaults to 10)
 *
 * This stage can be executed on shards and then reduced on the coordinator.
 * On each shard, it limits to the first n series, and during reduce, it combines
 * results from all shards and returns the first n series total.
 */
@PipelineStageAnnotation(name = "head")
public class HeadStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "head";
    /** The argument name for limit parameter. */
    public static final String LIMIT_ARG = "limit";

    private final int limit;

    /**
     * Constructs a new HeadStage with the specified limit.
     *
     * @param limit the number of series to return (defaults to 10 if not specified)
     */
    public HeadStage(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.limit = limit;
    }

    /**
     * Constructs a new HeadStage with default limit of 10.
     */
    public HeadStage() {
        this(10);
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return new ArrayList<>();
        }

        // Return the first limit series
        int size = Math.min(limit, input.size());
        return new ArrayList<>(input.subList(0, size));
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the limit.
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return false;
    }

    @Override
    public boolean isGlobalAggregation() {
        return true;
    }

    @Override
    public InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce) {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Aggregations list cannot be null or empty");
        }

        // Collect only the first limit series from all aggregations
        List<TimeSeries> resultSeries = new ArrayList<>(limit);
        for (TimeSeriesProvider aggregation : aggregations) {
            if (resultSeries.size() >= limit) {
                break; // Already have enough series
            }

            List<TimeSeries> aggregationSeries = aggregation.getTimeSeries();
            int remaining = limit - resultSeries.size();
            int toAdd = Math.min(remaining, aggregationSeries.size());

            // Add only the number of series we need
            for (int i = 0; i < toAdd; i++) {
                resultSeries.add(aggregationSeries.get(i));
            }
        }

        // Reuse first aggregation's metadata by using createReduced
        TimeSeriesProvider firstAgg = aggregations.get(0);
        TimeSeriesProvider result = firstAgg.createReduced(resultSeries);
        return (InternalAggregation) result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(LIMIT_ARG, limit);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(limit);
    }

    /**
     * Create a HeadStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new HeadStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static HeadStage readFrom(StreamInput in) throws IOException {
        int limit = in.readInt();
        return new HeadStage(limit);
    }

    /**
     * Create a HeadStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return HeadStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static HeadStage fromArgs(Map<String, Object> args) {
        if (args == null || args.isEmpty() || !args.containsKey(LIMIT_ARG)) {
            return new HeadStage(); // Default to 10
        }

        Object limitObj = args.get(LIMIT_ARG);
        if (limitObj == null) {
            return new HeadStage(); // Default to 10
        }

        int limit;
        if (limitObj instanceof Number number) {
            limit = number.intValue();
        } else if (limitObj instanceof String limitStr) {
            try {
                limit = Integer.parseInt(limitStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid type for '" + LIMIT_ARG + "' argument. Expected integer, but got: " + limitStr,
                    e
                );
            }
        } else {
            throw new IllegalArgumentException(
                "Invalid type for '" + LIMIT_ARG + "' argument. Expected Number or String, but got " + limitObj.getClass().getSimpleName()
            );
        }

        return new HeadStage(limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HeadStage other = (HeadStage) obj;
        return limit == other.limit;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(limit);
    }
}
