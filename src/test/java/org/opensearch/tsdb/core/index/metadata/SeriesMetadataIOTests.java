/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SeriesMetadataIOTests extends OpenSearchTestCase {

    public void testWriteAndReadSmallMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write metadata with 10 series
            Map<Long, Long> metadata = new HashMap<>();
            for (long i = 0; i < 10; i++) {
                metadata.put(i, i * 100);
            }

            String filename = SeriesMetadataIO.writeMetadata(dir, 1, metadata);
            assertEquals("series_metadata_1", filename);

            // Read and verify
            Map<Long, Long> readMetadata = SeriesMetadataIO.readMetadata(dir, filename);
            assertEquals(metadata.size(), readMetadata.size());
            for (Map.Entry<Long, Long> entry : metadata.entrySet()) {
                assertEquals(entry.getValue(), readMetadata.get(entry.getKey()));
            }
        }
    }

    public void testWriteAndReadLargeMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write metadata with 100K series
            Map<Long, Long> metadata = new HashMap<>();
            for (long i = 0; i < 100000; i++) {
                metadata.put(i, i * 1000);
            }

            String filename = SeriesMetadataIO.writeMetadata(dir, 2, metadata);
            assertEquals("series_metadata_2", filename);

            // Read and verify
            Map<Long, Long> readMetadata = SeriesMetadataIO.readMetadata(dir, filename);
            assertEquals(metadata.size(), readMetadata.size());

            // Spot check some values
            assertEquals(Long.valueOf(0), readMetadata.get(0L));
            assertEquals(Long.valueOf(50000 * 1000), readMetadata.get(50000L));
            assertEquals(Long.valueOf(99999 * 1000), readMetadata.get(99999L));
        }
    }

    public void testReadWithConsumer() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write metadata
            Map<Long, Long> metadata = new HashMap<>();
            metadata.put(1L, 100L);
            metadata.put(2L, 200L);
            metadata.put(3L, 300L);

            String filename = SeriesMetadataIO.writeMetadata(dir, 3, metadata);

            // Read with consumer
            Map<Long, Long> readMetadata = new HashMap<>();
            SeriesMetadataIO.readMetadata(dir, filename, readMetadata::put);

            assertEquals(metadata, readMetadata);
        }
    }

    public void testGenerateAndExtractFilename() {
        String filename = SeriesMetadataIO.generateFilename(42);
        assertEquals("series_metadata_16", filename); // 42 in base-36 is "16"

        long generation = SeriesMetadataIO.extractGeneration(filename);
        assertEquals(42, generation);
    }

    public void testExtractGenerationInvalidFilename() {
        assertThrows(IllegalArgumentException.class, () -> SeriesMetadataIO.extractGeneration("invalid_filename"));
    }

    public void testListMetadataFiles() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write multiple metadata files
            Map<Long, Long> metadata = Map.of(1L, 100L);
            SeriesMetadataIO.writeMetadata(dir, 1, metadata);
            SeriesMetadataIO.writeMetadata(dir, 2, metadata);
            SeriesMetadataIO.writeMetadata(dir, 5, metadata);

            List<String> files = SeriesMetadataIO.listMetadataFiles(dir);
            assertEquals(3, files.size());
            assertTrue(files.contains("series_metadata_1"));
            assertTrue(files.contains("series_metadata_2"));
            assertTrue(files.contains("series_metadata_5"));
        }
    }

    public void testCleanupOldFiles() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write multiple metadata files
            Map<Long, Long> metadata = Map.of(1L, 100L);
            SeriesMetadataIO.writeMetadata(dir, 1, metadata);
            SeriesMetadataIO.writeMetadata(dir, 2, metadata);
            SeriesMetadataIO.writeMetadata(dir, 3, metadata);
            SeriesMetadataIO.writeMetadata(dir, 4, metadata);
            SeriesMetadataIO.writeMetadata(dir, 5, metadata);

            // Protect files 3 and 5
            Set<String> protectedFiles = new HashSet<>();
            protectedFiles.add("series_metadata_3");
            protectedFiles.add("series_metadata_5");

            // Cleanup (should delete 1, 2, 4)
            SeriesMetadataIO.cleanupOldFiles(dir, protectedFiles);

            List<String> remainingFiles = SeriesMetadataIO.listMetadataFiles(dir);
            assertEquals(2, remainingFiles.size());
            assertTrue(remainingFiles.contains("series_metadata_3"));
            assertTrue(remainingFiles.contains("series_metadata_5"));
            assertFalse(remainingFiles.contains("series_metadata_1"));
            assertFalse(remainingFiles.contains("series_metadata_2"));
            assertFalse(remainingFiles.contains("series_metadata_4"));
        }
    }

    public void testEmptyMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Write empty metadata
            Map<Long, Long> metadata = new HashMap<>();
            String filename = SeriesMetadataIO.writeMetadata(dir, 1, metadata);

            // Read should return empty map
            Map<Long, Long> readMetadata = SeriesMetadataIO.readMetadata(dir, filename);
            assertTrue(readMetadata.isEmpty());
        }
    }

    public void testMissingFile() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            // Try to read non-existent file
            assertThrows(IOException.class, () -> SeriesMetadataIO.readMetadata(dir, "nonexistent_file"));
        }
    }
}
