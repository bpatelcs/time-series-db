/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for binary pipeline projection stages that provides common functionality
 * for label matching and time alignment operations when handling left and right time series operands.
 */
public abstract class AbstractBinaryProjectionStage implements BinaryPipelineStage {

    /**
     * Default constructor for AbstractBinaryProjectionStage.
     */
    protected AbstractBinaryProjectionStage() {}

    /** The parameter name for label keys. */
    public static final String LABELS_PARAM_KEY = "labels";

    protected abstract boolean hasKeepNansOption();

    /**
     * Find a time series in the list that matches the target labels for the provided label keys.
     * If we only have 1 time series, ignore labels.
     * If labelKeys is null or empty, performs full label matching.
     *
     * @param timeSeriesList The list of time series to search
     * @param targetLabels The target labels to match against
     * @param labelKeys The specific label keys to consider for matching, or null/empty for full matching
     * @return The matching time series, or null if no match found
     */
    protected List<TimeSeries> findMatchingTimeSeries(List<TimeSeries> timeSeriesList, Labels targetLabels, List<String> labelKeys) {
        List<TimeSeries> matchingTimeSeriesList = new ArrayList<>();
        for (TimeSeries timeSeries : timeSeriesList) {
            if (labelsMatch(targetLabels, timeSeries.getLabels(), labelKeys)) {
                matchingTimeSeriesList.add(timeSeries);
            }
        }
        return matchingTimeSeriesList;
    }

    /**
     * Check if two Labels objects match for all specified label keys.
     * If labelKeys is null or empty, performs full label matching.
     *
     * @param leftLabels The left labels
     * @param rightLabels The right labels
     * @param labelKeys The specific label keys to consider for matching, or null/empty for full matching
     * @return true if labels match for all specified keys, false otherwise
     */
    protected boolean labelsMatch(Labels leftLabels, Labels rightLabels, List<String> labelKeys) {
        if (leftLabels == null || rightLabels == null) {
            return false;
        }

        // If no specific tag provided, use full label matching
        if (labelKeys == null || labelKeys.isEmpty()) {
            return leftLabels.equals(rightLabels);
        }

        // Check that all specified label keys match
        for (String labelKey : labelKeys) {
            String leftValue = leftLabels.get(labelKey);
            String rightValue = rightLabels.get(labelKey);

            if (leftValue.equals(rightValue) == false) {
                return false;
            }
        }

        return true;
    }

    /**
     * Align two time series by timestamp and process the samples.
     * Subclass will decide if they want to include NaN value from left/right series.
     * Both left and right time series are expected to be sorted by timestamp.
     *
     * @param leftSeries The left time series
     * @param rightSeries The right time series
     * @return A new time series, or null if no matching timestamps are found.
     */
    protected TimeSeries alignAndProcess(TimeSeries leftSeries, TimeSeries rightSeries) {
        if (leftSeries == null || rightSeries == null) {
            return null;
        }

        List<Sample> leftSamples = leftSeries.getSamples();
        List<Sample> rightSamples = rightSeries.getSamples();

        if (leftSamples == null || rightSamples == null) {
            return null;
        }

        List<Sample> resultSamples = new ArrayList<>();
        boolean hasKeepNansOptions = hasKeepNansOption();

        // Find matching timestamps between the two sorted time series.
        // The input time series is expected to be sorted by timestamp in increasing order.
        int leftIndex = 0;
        int rightIndex = 0;

        while (leftIndex < leftSamples.size() || rightIndex < rightSamples.size()) {
            Sample leftSample = null;
            Sample rightSample = null;
            Long leftTimestamp = Long.MAX_VALUE;
            Long rightTimestamp = Long.MAX_VALUE;
            if (leftIndex < leftSamples.size()) {
                leftSample = leftSamples.get(leftIndex);
                leftTimestamp = leftSample.getTimestamp();
            }
            if (rightIndex < rightSamples.size()) {
                rightSample = rightSamples.get(rightIndex);
                rightTimestamp = rightSample.getTimestamp();
            }

            Sample resultSample;
            if (leftTimestamp < rightTimestamp) {
                // If stage doesn't have keepNans option, we skip processing
                if (hasKeepNansOptions) {
                    resultSample = processSamples(leftSample, null);
                } else {
                    resultSample = null;
                }
                leftIndex++;

            } else if (rightTimestamp < leftTimestamp) {
                // If stage doesn't have keepNans option, we skip processing
                if (hasKeepNansOptions) {
                    resultSample = processSamples(null, rightSample);
                } else {
                    resultSample = null;
                }
                rightIndex++;
            } else {
                resultSample = processSamples(leftSample, rightSample);
                leftIndex++;
                rightIndex++;
            }
            if (resultSample != null) {
                resultSamples.add(resultSample);
            }
        }

        if (resultSamples.isEmpty()) {
            return null;
        }

        // Calculate min/max timestamps from the union of both series
        long minTimestamp = Math.min(leftSeries.getMinTimestamp(), rightSeries.getMinTimestamp());
        long maxTimestamp = Math.max(leftSeries.getMaxTimestamp(), rightSeries.getMaxTimestamp());

        // Transform labels if needed (can be overridden by subclasses)
        Labels transformedLabels = transformLabels(leftSeries.getLabels());

        return new TimeSeries(resultSamples, transformedLabels, minTimestamp, maxTimestamp, leftSeries.getStep(), leftSeries.getAlias());
    }

    /**
     * Get the label keys to use for selective matching.
     * If null or empty, full label matching will be performed.
     *
     * @return The list of label keys for selective matching, or null for full matching
     */
    protected abstract List<String> getLabelKeys();

    /**
     * Process two time series inputs and return the resulting time series aligning timestamps and matching labels.
     * When labelKeys are specified, standard label matching is always used regardless of the number of right series.
     * When no labelKeys are specified and there's a single right series, all left series are processed against it.
     *
     * @param left The left operand time series
     * @param right The right operand time series
     * @return The result time series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left == null) {
            throw new NullPointerException(getName() + " stage received null left input");
        }
        if (right == null) {
            throw new NullPointerException(getName() + " stage received null right input");
        }
        if (left.isEmpty() || right.isEmpty()) {
            return new ArrayList<>();
        }

        // If no label keys are provided and right operand has single series, project all left operand time series onto
        // the right time series without label matching.
        if (right.size() == 1) {
            return processWithoutLabelMatching(left, right.getFirst());
        } else {
            return processWithLabelMatching(left, right);
        }
    }

    /**
     * Process left time series against a single right time series without label matching.
     * This method is called when no labelKeys are specified and a single right time series is provided.
     *
     * @param left The left operand time series list
     * @param rightSeries The single right operand time series
     * @return The result time series list
     */
    protected List<TimeSeries> processWithoutLabelMatching(List<TimeSeries> left, TimeSeries rightSeries) {
        List<TimeSeries> result = new ArrayList<>();

        for (TimeSeries leftSeries : left) {
            TimeSeries processedSeries = alignAndProcess(leftSeries, rightSeries);
            if (processedSeries != null) {
                result.add(processedSeries);
            }
        }

        return result;
    }

    /**
     * Process left time series against multiple right time series.
     * Matches time series by labels using selective matching if labelKeys are provided. Otherwise match entire labels set.
     *
     * @param left The left operand time series list
     * @param right The right operand time series list
     * @return The result time series list
     */
    protected List<TimeSeries> processWithLabelMatching(List<TimeSeries> left, List<TimeSeries> right) {
        List<TimeSeries> result = new ArrayList<>();
        // TODO we need to check intersect of left and right on common tags and use them when no grouping labels
        List<String> labelKeys = getLabelKeys();

        for (TimeSeries leftSeries : left) {
            // TODO repeated extracting labels from right series, we should fix by grouping first and iterating over group
            List<TimeSeries> matchingRightSeriesList = findMatchingTimeSeries(right, leftSeries.getLabels(), labelKeys);
            TimeSeries matchingRightSeries = mergeMatchingSeries(matchingRightSeriesList);
            if (matchingRightSeries != null) {
                TimeSeries processedSeries = alignAndProcess(leftSeries, matchingRightSeries);
                if (processedSeries != null) {
                    result.add(processedSeries);
                }
            }
        }

        return result;
    }

    protected abstract TimeSeries mergeMatchingSeries(List<TimeSeries> rightTimeSeries);

    /**
     * Transform labels before creating the result time series.
     * The default implementation returns labels unchanged.
     * Subclasses can override this to add, modify, or remove labels as needed.
     *
     * @param originalLabels The original labels from the left series
     * @return The transformed labels to use in the result time series
     */
    protected Labels transformLabels(Labels originalLabels) {
        return originalLabels;
    }

    /**
     * Process samples from left and right time series and return a result sample.
     * This method should be overridden by subclasses to implement their specific logic.
     * Both samples are expected to be non-null and have matching timestamps.
     *
     * @param leftSample The left sample
     * @param rightSample The right sample
     * @return The result sample
     */
    protected abstract Sample processSamples(Sample leftSample, Sample rightSample);

    @Override
    public int hashCode() {
        List<String> labelKeys = getLabelKeys();
        return labelKeys != null ? labelKeys.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractBinaryProjectionStage that = (AbstractBinaryProjectionStage) obj;
        List<String> labelKeys = getLabelKeys();
        List<String> thatLabelKeys = that.getLabelKeys();
        if (labelKeys == null && thatLabelKeys == null) {
            return true;
        }
        if (labelKeys == null || thatLabelKeys == null) {
            return false;
        }
        return labelKeys.equals(thatLabelKeys);
    }
}
