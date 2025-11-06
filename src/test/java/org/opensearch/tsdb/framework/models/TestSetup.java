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
import java.util.Map;

/**
 * Test setup configuration for time series testing.
 * Supports multiple index configurations for complex test scenarios.
 * Note: Index mapping and settings are provided by the framework, only name/shards/replicas are configurable
 */
public record TestSetup(@JsonProperty("name") String name, @JsonProperty("description") String description,
    @JsonProperty("cluster_config") ClusterConfig clusterConfig, @JsonProperty("index_configs") List<IndexConfig> indexConfigs,
    @JsonProperty("node_settings") Map<String, Object> nodeSettings) {
}
