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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.stage.summarize.AvgBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.BucketMapper;
import org.opensearch.tsdb.lang.m3.stage.summarize.BucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.LastBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.MaxBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.MinBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.PercentileBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.StdDevBucketSummarizer;
import org.opensearch.tsdb.lang.m3.stage.summarize.SumBucketSummarizer;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Pipeline stage that summarizes time series data into interval buckets.
 *
 * <p>This stage groups data points into fixed-size time intervals (buckets) and applies
 * an aggregation function to each bucket. This is useful for downsampling high-resolution
 * data or aggregating counter increments over time windows.</p>
 *
 * <h2>Parameters:</h2>
 * <ul>
 *   <li><strong>interval:</strong> Duration of each bucket in the same time unit as sample timestamps</li>
 *   <li><strong>function:</strong> Aggregation function (sum, avg, max, min, last, stddev, p0-p100) - required</li>
 *   <li><strong>alignToFrom:</strong> If true, align buckets to query start time; if false, round to interval boundaries</li>
 * </ul>
 *
 * <h2>Alignment Behavior:</h2>
 * <ul>
 *   <li><strong>alignToFrom=false (default):</strong> Buckets align to interval boundaries.
 *       Example: With 1-hour interval, 22:32 falls in bucket 22:00-23:00</li>
 *   <li><strong>alignToFrom=true:</strong> Buckets start from query start time.
 *       Example: If query starts at 6:30, 22:32 falls in bucket 22:30-23:30</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "summarize")
public class SummarizeStage implements UnaryPipelineStage {

    /** The name identifier for this stage. */
    public static final String NAME = "summarize";

    /** Interval duration in the same time unit as sample timestamps. */
    private final long interval;

    /** Aggregation function type. */
    private final WindowAggregationType function;

    /** Whether to align buckets to query start time. */
    private final boolean alignToFrom;

    /** Factory to create new BucketSummarizer instances for this function. */
    private final Supplier<BucketSummarizer> summarizerFactory;

    /**
     * Full constructor with all parameters.
     * Creates the summarizer factory based on the WindowAggregationType.
     *
     * @param interval bucket interval in the same time unit as sample timestamps
     * @param function aggregation function type
     * @param alignToFrom whether to align buckets to query start time
     * @throws IllegalArgumentException if interval &lt;= 0 or function is null
     */
    public SummarizeStage(long interval, WindowAggregationType function, boolean alignToFrom) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        if (function == null) {
            throw new IllegalArgumentException("Function cannot be null");
        }

        this.interval = interval;
        this.function = function;
        this.alignToFrom = alignToFrom;
        // Create the summarizer factory based on the function type
        this.summarizerFactory = createSummarizerFactory(function);
    }

    /**
     * Create a factory for creating BucketSummarizer instances based on the WindowAggregationType.
     *
     * @param function the window aggregation type
     * @return a supplier that creates new BucketSummarizer instances
     */
    private static Supplier<BucketSummarizer> createSummarizerFactory(WindowAggregationType function) {
        return switch (function.getType()) {
            case SUM -> SumBucketSummarizer::new;
            case AVG -> AvgBucketSummarizer::new;
            case MAX -> MaxBucketSummarizer::new;
            case MIN -> MinBucketSummarizer::new;
            case LAST -> LastBucketSummarizer::new;
            case STDDEV -> StdDevBucketSummarizer::new;
            case PERCENTILE -> {
                float percentile = function.getPercentileValue();
                yield () -> new PercentileBucketSummarizer(percentile);
            }
            default -> throw new IllegalArgumentException("Unsupported function type: " + function);
        };
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries series : input) {
            result.add(processSeries(series));
        }
        return result;
    }

    /**
     * Process a single time series, summarizing its samples into buckets.
     */
    private TimeSeries processSeries(TimeSeries series) {
        List<Sample> samples = series.getSamples();
        if (samples.isEmpty()) {
            return series;
        }

        long seriesStep = series.getStep();

        // Validate that interval >= series resolution
        if (interval < seriesStep) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Summarize interval (%d) must be >= series resolution (%d)", interval, seriesStep)
            );
        }

        long minTimestamp = series.getMinTimestamp();
        long maxTimestamp = series.getMaxTimestamp();

        // Create bucket mapper for this time series
        BucketMapper bucketMapper = alignToFrom
            ? new BucketMapper(interval, minTimestamp)  // Align to series start time
            : new BucketMapper(interval);                // Align to fixed reference

        // Calculate bucket boundaries
        long bucketStart = bucketMapper.calculateBucketStart(minTimestamp);
        long bucketEnd = bucketMapper.calculateBucketEnd(maxTimestamp);

        // Calculate number of buckets
        // bucketEnd is exclusive and always aligned, so simple division works
        int numBuckets = (int) ((bucketEnd - bucketStart) / interval);
        List<Sample> result = new ArrayList<>(numBuckets);

        // Process each bucket
        BucketSummarizer summarizer = summarizerFactory.get();
        long currentBucketStart = bucketStart;
        int sampleIdx = 0;

        for (int i = 0; i < numBuckets; i++) {
            long currentBucketEnd = currentBucketStart + interval;
            summarizer.reset();

            // Accumulate all samples that fall within this bucket
            while (sampleIdx < samples.size()) {
                Sample sample = samples.get(sampleIdx);
                long sampleTimestamp = sample.getTimestamp();

                if (sampleTimestamp >= currentBucketEnd) {
                    break; // This sample belongs to a future bucket
                }

                if (sampleTimestamp >= currentBucketStart) {
                    // Only add if sample exists (null values are represented by absence)
                    summarizer.accumulate(sample.getValue());
                }

                sampleIdx++;
            }

            // Add result if we have data in this bucket
            if (summarizer.hasData()) {
                result.add(new FloatSample(currentBucketStart, summarizer.finish()));
            }

            currentBucketStart = currentBucketEnd;
        }

        // Calculate new metadata for the summarized time series using bucket mapper
        long newMinTimestamp = bucketMapper.mapToBucket(minTimestamp);
        long newMaxTimestamp = bucketMapper.mapToBucket(maxTimestamp);
        long newStep = interval;

        return new TimeSeries(result, series.getLabels(), newMinTimestamp, newMaxTimestamp, newStep, series.getAlias());
    }

    @Override
    public void toXContent(XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params) throws IOException {
        builder.field("interval", interval);
        builder.field("function", function.toString().toLowerCase(Locale.ROOT));
        builder.field("alignToFrom", alignToFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(interval);
        function.writeTo(out);
        out.writeBoolean(alignToFrom);
    }

    /**
     * Read a SummarizeStage from the input stream.
     */
    public static SummarizeStage readFrom(StreamInput in) throws IOException {
        long interval = in.readLong();
        WindowAggregationType function = WindowAggregationType.readFrom(in);
        boolean alignToFrom = in.readBoolean();
        return new SummarizeStage(interval, function, alignToFrom);
    }

    /**
     * Create a SummarizeStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return SummarizeStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static SummarizeStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        // Parse interval (required)
        Object intervalObj = args.get("interval");
        if (intervalObj == null) {
            throw new IllegalArgumentException("interval argument is required");
        }

        if (!(intervalObj instanceof Number)) {
            throw new IllegalArgumentException("interval must be a number");
        }
        long interval = ((Number) intervalObj).longValue();

        // Parse function (required)
        if (!args.containsKey("function")) {
            throw new IllegalArgumentException("function argument is required");
        }
        Object functionObj = args.get("function");
        if (!(functionObj instanceof String)) {
            throw new IllegalArgumentException("function must be a string");
        }
        WindowAggregationType function = WindowAggregationType.fromString((String) functionObj);

        // Parse alignToFrom (optional, default: false)
        boolean alignToFrom = false;
        if (args.containsKey("alignToFrom")) {
            Object alignObj = args.get("alignToFrom");
            if (!(alignObj instanceof Boolean)) {
                throw new IllegalArgumentException("alignToFrom must be a boolean");
            }
            alignToFrom = (Boolean) alignObj;
        }

        return new SummarizeStage(interval, function, alignToFrom);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SummarizeStage that = (SummarizeStage) obj;
        return interval == that.interval && alignToFrom == that.alignToFrom && Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, function, alignToFrom);
    }
}
