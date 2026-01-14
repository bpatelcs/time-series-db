/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * HeadPlanNode represents a plan node that handles head operations in M3QL.
 *
 * The head function returns the first n series from the series list.
 * It takes an optional limit argument (defaults to 10).
 *
 * This is a coordinator-only stage that operates on all time series at once.
 */
public class HeadPlanNode extends M3PlanNode {
    private final int limit;

    /**
     * Constructor for HeadPlanNode.
     *
     * @param id    The node ID
     * @param limit The number of series to return (defaults to 10 if not specified)
     */
    public HeadPlanNode(int id, int limit) {
        super(id);
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.limit = limit;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "HEAD(%d)", limit);
    }

    /**
     * Returns the limit.
     *
     * @return The limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Creates a HeadPlanNode from a FunctionNode.
     * Expected format:
     * - head() -> defaults to 10
     * - head(5) -> returns first 5 series
     *
     * @param functionNode The function node to parse
     * @return HeadPlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static HeadPlanNode of(FunctionNode functionNode) {
        int limit = 10; // Default

        if (!functionNode.getChildren().isEmpty()) {
            if (functionNode.getChildren().size() > 1) {
                throw new IllegalArgumentException("head function accepts at most 1 argument: limit");
            }

            M3ASTNode firstChild = functionNode.getChildren().getFirst();
            if (!(firstChild instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("head limit argument must be a numeric value");
            }

            try {
                limit = Integer.parseInt(valueNode.getValue());
                if (limit <= 0) {
                    throw new IllegalArgumentException("head limit must be positive, got: " + limit);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("head limit must be a valid integer, got: " + valueNode.getValue(), e);
            }
        }

        return new HeadPlanNode(M3PlannerContext.generateId(), limit);
    }
}
