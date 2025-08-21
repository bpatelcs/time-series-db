/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Time series data chunk storage and compression implementations.
 *
 * Key components:
 * - Chunk interfaces and implementations
 * - Compression algorithms (XOR)
 * - Chunk iterator for data access
 * - Chunk appender for data writing
 * - Encoding and decoding utilities
 */
package org.opensearch.tsdb.core.chunks;
