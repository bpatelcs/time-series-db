/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UnionStageTests extends OpenSearchTestCase {

    public void testUnion() {
        UnionStage stage = new UnionStage("right_series");

        // Left series
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        ByteLabels leftLabels1 = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 2000L, 1000L, "left-1");

        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 30.0), new FloatSample(2000L, 40.0));
        ByteLabels leftLabels2 = ByteLabels.fromMap(Map.of("service", "db"));
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 2000L, 1000L, "common");

        // Right series
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 50.0), new FloatSample(2000L, 60.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "cache"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 2000L, 1000L, "right-1");

        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 30.0), new FloatSample(2000L, 40.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 2000L, 1000L, "common");

        List<TimeSeries> left = new ArrayList<>(Arrays.asList(leftSeries1, leftSeries2));
        List<TimeSeries> right = new ArrayList<>(Arrays.asList(rightSeries1, rightSeries2));
        List<TimeSeries> result = stage.process(left, right);

        // Verify union contains all series. It includes the duplicate time series coming from left and right.
        assertEquals(4, result.size());
        assertEquals(leftSeries1, result.get(0));
        assertEquals(leftSeries2, result.get(1));
        assertEquals(rightSeries1, result.get(2));
        assertEquals(rightSeries2, result.get(3));
    }

    public void testUnionWithBothEmpty() {
        UnionStage stage = new UnionStage("right_series");

        List<TimeSeries> left = new ArrayList<>();
        List<TimeSeries> right = new ArrayList<>();
        List<TimeSeries> result = stage.process(left, right);

        // Should return empty list
        assertTrue(result.isEmpty());
    }

    public void testGetName() {
        UnionStage stage = new UnionStage("test_reference");
        assertEquals("union", stage.getName());
        assertEquals(UnionStage.NAME, stage.getName());
    }

    public void testGetRightOpReferenceName() {
        UnionStage stage = new UnionStage("test_reference");
        assertEquals("test_reference", stage.getRightOpReferenceName());
    }

    public void testSerialization() throws IOException {
        UnionStage stage = new UnionStage("test_reference");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                UnionStage readStage = UnionStage.readFrom(in);
                assertEquals("test_reference", readStage.getRightOpReferenceName());
                assertEquals("union", readStage.getName());
            }
        }
    }

    public void testToXContent() throws IOException {
        // Test toXContent
        UnionStage stage = new UnionStage("test_reference");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
    }

    public void testFromArgsAndSerialization() throws IOException {
        Map<String, Object> args = Map.of("right_op_reference", "test_series");
        UnionStage stage = UnionStage.fromArgs(args);
        assertEquals("test_series", stage.getRightOpReferenceName());
        assertEquals("union", stage.getName());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                UnionStage readStage = UnionStage.readFrom(in);
                assertEquals("test_series", readStage.getRightOpReferenceName());
                assertEquals("union", readStage.getName());
            }
        }
    }
}
