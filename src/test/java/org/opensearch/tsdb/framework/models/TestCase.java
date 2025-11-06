/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Test case configuration for time series testing.
 * Supports multiple input data configurations to ingest data into different indices.
 */
public record TestCase(@JsonProperty("name") String name, @JsonProperty("input_data_list") List<InputDataConfig> inputDataList,
    @JsonProperty("queries") List<QueryConfig> queries, @JsonProperty("validation") ValidationConfig validation) {
}
