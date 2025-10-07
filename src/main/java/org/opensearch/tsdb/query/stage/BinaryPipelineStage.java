/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;

/**
 * Interface for binary pipeline stages that operate on two time series inputs. This is a variant of {@link PipelineStage}.
 *
 * <p>Binary pipeline stages perform operations between two time series, such as
 * mathematical operations, comparisons, and set operations. They require two
 * inputs (left and right operands) and produce a single output.</p>
 *
 * <p>Note that binary pipeline stages can modify the input time series lists.</p>
 */
public interface BinaryPipelineStage extends PipelineStage {

    /**
     * The parameter name for the right operand reference in binary pipeline stages.
     */
    String RIGHT_OP_REFERENCE_PARAM_KEY = "right_op_reference";

    /**
     * Process two time series inputs and return the result.
     *
     * @param left The left operand time series. This must be a mutable list.
     * @param right The right operand time series. This must be a mutable list.
     * @return The result time series
     */
    List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right);

    /**
     * Get the reference name for the right operand of this binary stage.
     *
     * <p>This method should return the name of the aggregation reference that
     * should be used as the right operand for this binary operation. This is
     * used by the coordinator to resolve which aggregation result should be
     * used as the right operand.</p>
     *
     * @return The reference name for the right operand
     */
    String getRightOpReferenceName();

    /**
     * Binary pipeline stage requires two operands. This is not supported.
     */
    @Override
    default List<TimeSeries> process(List<TimeSeries> input) {
        throw new UnsupportedOperationException("Binary pipeline stage '" + getName() + "' requires two inputs");
    }

    /**
     * Returns if this stage is allowed to be executed only at the coordinator level.
     *
     * <p>By default, binary stages must be executed at the coordinator level since they require
     * combining data from multiple aggregations that are only available after shard-level processing.</p>
     */
    @Override
    default boolean isCoordinatorOnly() {
        return true;
    }
}
