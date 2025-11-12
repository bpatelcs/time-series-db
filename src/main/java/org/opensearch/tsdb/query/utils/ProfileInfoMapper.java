/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProfileInfoMapper {

    public static final String PROFILE_FIELD_NAME = "profile";
    public static final String TOTALS_FIELD_NAME = "totals";
    public static final String TOTAL_CHUNKS = "total_chunks";
    public static final String TOTAL_SAMPLES = "total_samples";
    public static final String TOTAL_INPUT_SERIES = "total_input_series";
    public static final String TOTAL_OUTPUT_SERIES = "total_output_series";
    public static final String LIVE_CHUNK_COUNT = "live_chunk_count";
    public static final String CLOSED_CHUNK_COUNT = "closed_chunk_count";
    public static final String LIVE_DOC_COUNT = "live_doc_count";
    public static final String CLOSED_DOC_COUNT = "closed_doc_count";
    public static final String LIVE_SAMPLE_COUNT = "live_sample_count";
    public static final String CLOSED_SAMPLE_COUNT = "closed_sample_count";
    public static final String STAGES_FIELD_NAME = "stages";
    public static final String DEBUG_INFO_FIELD_NAME = "debug_info";
    public static final String DEFAULT_STAGES = "fetch_only";
    public static final String SHARDS_FIELD_NAME = "shards";
    public static final String SHARD_ID_FIELD_NAME = "shard_id";
    public static final String AGGREGATIONS_FIELD_NAME = "aggregations";

    /**
     * Extract Profile Info from TimeSeriesUnfoldAggregator and construct debug info for every stage
     * @param response
     * @param builder
     * @throws IOException
     */
    public static void extractProfileInfo(SearchResponse response, XContentBuilder builder) throws IOException {
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        if (profileResults != null && !profileResults.isEmpty()) {
            // Aggregate debug info from all shards
            DebugStats debugStats = new DebugStats();
            for (Map.Entry<String, ProfileShardResult> entry : profileResults.entrySet()) {
                String shardId = entry.getKey();
                ProfileShardResult shardResult = entry.getValue();
                PerShardStats perShardStats = extractPerShardStats(shardId, shardResult, debugStats);
                if (!perShardStats.aggregations.isEmpty()) {
                    debugStats.shardStats.add(perShardStats);
                }

            }

            builder.startObject(PROFILE_FIELD_NAME);
            // Add debug info section
            writeDebugInfoToXContent(builder, debugStats);

            builder.endObject();
        }
    }

    private static PerShardStats extractPerShardStats(String shardId, ProfileShardResult shardResult, DebugStats debugStats) {
        // Extract debug information per shard
        PerShardStats shardStats = new PerShardStats(shardId);
        if (shardResult.getAggregationProfileResults() != null) {
            for (ProfileResult profileResult : shardResult.getAggregationProfileResults().getProfileResults()) {
                AggregationStats aggStats = extractPerAggregation(profileResult, debugStats);
                if (aggStats.stages != null) {
                    shardStats.aggregations.add(aggStats);
                }
            }
        }
        return shardStats;
    }

    private static AggregationStats extractPerAggregation(ProfileResult profileResult, DebugStats stats) {
        AggregationStats aggStats = new AggregationStats();
        // Check if this is a time_series_unfold aggregation
        if (TimeSeriesUnfoldAggregator.class.getName().equals(profileResult.getQueryName())) {
            Map<String, Object> debugMap = profileResult.getDebugInfo();
            if (debugMap != null && !debugMap.isEmpty()) {
                // Extract stages metadata
                String stages = (String) debugMap.get(STAGES_FIELD_NAME);
                if (stages == null || stages.isEmpty()) {
                    stages = DEFAULT_STAGES;
                }
                // Populate aggregation stats
                aggStats.description = profileResult.getLuceneDescription();
                aggStats.stages = stages;
                aggStats.chunkCount = getLongValue(debugMap, TOTAL_CHUNKS);
                aggStats.sampleCount = getLongValue(debugMap, TOTAL_SAMPLES);
                aggStats.liveChunksCount = getLongValue(debugMap, LIVE_CHUNK_COUNT);
                aggStats.closedChunksCount = getLongValue(debugMap, CLOSED_CHUNK_COUNT);
                aggStats.liveDocCount = getLongValue(debugMap, LIVE_DOC_COUNT);
                aggStats.closedDocCount = getLongValue(debugMap, CLOSED_DOC_COUNT);
                aggStats.liveSampleCount = getLongValue(debugMap, LIVE_SAMPLE_COUNT);
                aggStats.closedSampleCount = getLongValue(debugMap, CLOSED_SAMPLE_COUNT);
                aggStats.inputSeriesCount = getLongValue(debugMap, TOTAL_INPUT_SERIES);
                aggStats.outputSeriesCount = getLongValue(debugMap, TOTAL_OUTPUT_SERIES);

                // Also accumulate into totals
                stats.totals.totalTime += profileResult.getTime();
                stats.totals.chunkCount += getLongValue(debugMap, TOTAL_CHUNKS);
                stats.totals.sampleCount += getLongValue(debugMap, TOTAL_SAMPLES);
                stats.totals.liveChunksCount += getLongValue(debugMap, LIVE_CHUNK_COUNT);
                stats.totals.closedChunksCount += getLongValue(debugMap, CLOSED_CHUNK_COUNT);
                stats.totals.liveDocCount += getLongValue(debugMap, LIVE_DOC_COUNT);
                stats.totals.closedDocCount += getLongValue(debugMap, CLOSED_DOC_COUNT);
                stats.totals.liveSampleCount += getLongValue(debugMap, LIVE_SAMPLE_COUNT);
                stats.totals.closedSampleCount += getLongValue(debugMap, CLOSED_SAMPLE_COUNT);
                stats.totals.inputSeriesCount += getLongValue(debugMap, TOTAL_INPUT_SERIES);
                stats.totals.outputSeriesCount += getLongValue(debugMap, TOTAL_OUTPUT_SERIES);
            }
        }
        return aggStats;
    }

    private static void writeDebugInfoToXContent(XContentBuilder builder, DebugStats debugStats) throws IOException {
        if (!debugStats.shardStats.isEmpty()) {
            builder.startObject(TOTALS_FIELD_NAME);
            writeAggregationFields(builder, debugStats.totals);
            builder.endObject();

            builder.startArray(SHARDS_FIELD_NAME);
            for (PerShardStats shardStats : debugStats.shardStats) {
                writeShardStats(builder, shardStats);
            }
            builder.endArray();
        }
    }

    /**
     * Writes stage statistics to the XContentBuilder.
     */
    private static void writeShardStats(XContentBuilder builder, PerShardStats stats) throws IOException {
        builder.startObject();
        builder.field(SHARD_ID_FIELD_NAME, stats.shardId);
        builder.startArray(AGGREGATIONS_FIELD_NAME);
        for (AggregationStats aggStats : stats.aggregations) {
            writeAggregationStats(builder, aggStats);
        }
        builder.endArray();

        builder.endObject();
    }

    /**
     * Writes aggregation stats as an object (for arrays)
     */
    private static void writeAggregationStats(XContentBuilder builder, AggregationStats stats) throws IOException {
        builder.startObject();
        writeAggregationFields(builder, stats);
        builder.endObject();
    }

    /**
     * Writes aggregation stat fields (without wrapping object)
     */
    private static void writeAggregationFields(XContentBuilder builder, AggregationStats stats) throws IOException {
        builder.startObject(DEBUG_INFO_FIELD_NAME);
        if (stats.stages != null && !stats.stages.isEmpty()) {
            builder.field(STAGES_FIELD_NAME, stats.stages);
        }
        builder.field(TOTAL_CHUNKS, stats.chunkCount);
        builder.field(TOTAL_SAMPLES, stats.sampleCount);
        builder.field(TOTAL_INPUT_SERIES, stats.inputSeriesCount);
        builder.field(TOTAL_OUTPUT_SERIES, stats.outputSeriesCount);
        builder.field(LIVE_CHUNK_COUNT, stats.liveChunksCount);
        builder.field(CLOSED_CHUNK_COUNT, stats.closedChunksCount);
        builder.field(LIVE_DOC_COUNT, stats.liveDocCount);
        builder.field(CLOSED_DOC_COUNT, stats.closedDocCount);
        builder.field(LIVE_SAMPLE_COUNT, stats.liveSampleCount);
        builder.field(CLOSED_SAMPLE_COUNT, stats.closedSampleCount);
        builder.endObject();
    }

    private static long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    private static class DebugStats {
        List<PerShardStats> shardStats = new ArrayList<>();
        AggregationStats totals = new AggregationStats();
    }

    private static class PerShardStats {
        String shardId;
        List<AggregationStats> aggregations = new ArrayList<>();

        PerShardStats(String shardId) {
            this.shardId = shardId;
        }
    }

    private static class AggregationStats {
        String description;
        String stages;
        long totalTime = 0;
        long chunkCount = 0;
        long sampleCount = 0;
        long inputSeriesCount = 0;
        long outputSeriesCount = 0;
        long liveChunksCount = 0;
        long closedChunksCount = 0;
        long liveDocCount = 0;
        long closedDocCount = 0;
        long liveSampleCount = 0;
        long closedSampleCount = 0;
    }

}
