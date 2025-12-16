/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility class for reading and writing live series metadata to separate files.
 * This prevents bloating of Lucene's segments_N files with large metadata.
 */
public class SeriesMetadataIO {

    private static final Logger logger = LogManager.getLogger(SeriesMetadataIO.class);
    private static final String CODEC_NAME = "SeriesMetadata";
    private static final int VERSION_1 = 1;
    private static final int VERSION_CURRENT = VERSION_1;
    private static final String FILE_PREFIX = "series_metadata_";

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private SeriesMetadataIO() {
        // Utility class
    }

    /**
     * Write metadata to a file in the given directory.
     *
     * @param dir the Lucene directory to write to
     * @param generation the commit generation number
     * @param metadata the map of series references to timestamps
     * @return the filename that was written
     * @throws IOException if writing fails
     */
    public static String writeMetadata(Directory dir, long generation, Map<Long, Long> metadata) throws IOException {
        String filename = generateFilename(generation);

        try (IndexOutput output = dir.createOutput(filename, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, CODEC_NAME, VERSION_CURRENT);

            output.writeVLong(metadata.size());
            for (Map.Entry<Long, Long> entry : metadata.entrySet()) {
                output.writeLong(entry.getKey());   // series reference
                output.writeVLong(entry.getValue()); // timestamp
            }

            CodecUtil.writeFooter(output);
        }

        return filename;
    }

    /**
     * Read metadata from a file and return as a Map.
     *
     * @param dir the Lucene directory to read from
     * @param filename the filename to read
     * @return a Map of series references to timestamps
     * @throws IOException if reading fails or checksum is invalid
     */
    public static Map<Long, Long> readMetadata(Directory dir, String filename) throws IOException {
        Map<Long, Long> result = new java.util.HashMap<>();
        readMetadata(dir, filename, result::put);
        return result;
    }

    /**
     * Read metadata from a file and stream to a consumer.
     *
     * @param dir the Lucene directory to read from
     * @param filename the filename to read
     * @param consumer a BiConsumer that accepts (seriesRef, timestamp) pairs
     * @throws IOException if reading fails or checksum is invalid
     */
    public static void readMetadata(Directory dir, String filename, BiConsumer<Long, Long> consumer) throws IOException {
        try (ChecksumIndexInput input = dir.openChecksumInput(filename)) {
            CodecUtil.checkHeader(input, CODEC_NAME, VERSION_1, VERSION_CURRENT);

            long numSeries = input.readVLong();
            for (long i = 0; i < numSeries; i++) {
                long seriesRef = input.readLong();
                long timestamp = input.readVLong();
                consumer.accept(seriesRef, timestamp);
            }

            CodecUtil.checkFooter(input);
        }
    }

    /**
     * Generate a filename for a given generation number.
     *
     * @param generation the commit generation number
     * @return the filename in format "series_metadata_{generation}" where generation is base-36 encoded
     */
    public static String generateFilename(long generation) {
        return FILE_PREFIX + Long.toString(generation, Character.MAX_RADIX);
    }

    /**
     * Extract the generation number from a metadata filename.
     *
     * @param filename the filename
     * @return the generation number
     * @throws NumberFormatException if the filename format is invalid
     */
    public static long extractGeneration(String filename) {
        if (!filename.startsWith(FILE_PREFIX)) {
            throw new IllegalArgumentException("Invalid metadata filename: " + filename);
        }
        return Long.parseLong(filename.substring(FILE_PREFIX.length()), Character.MAX_RADIX);
    }

    /**
     * List all metadata files in the directory.
     *
     * @param dir the Lucene directory
     * @return a list of metadata filenames
     * @throws IOException if listing fails
     */
    public static List<String> listMetadataFiles(Directory dir) throws IOException {
        List<String> metadataFiles = new ArrayList<>();
        String[] allFiles = dir.listAll();

        for (String file : allFiles) {
            if (file.startsWith(FILE_PREFIX)) {
                metadataFiles.add(file);
            }
        }

        return metadataFiles;
    }

    /**
     * Delete old metadata files, excluding protected files.
     * This method will delete all metadata files that are not in the protected set.
     *
     * @param dir the Lucene directory
     * @param protectedFiles set of filenames that should not be deleted
     * @throws IOException if deletion fails
     */
    public static void cleanupOldFiles(Directory dir, Set<String> protectedFiles) throws IOException {
        List<String> metadataFiles = listMetadataFiles(dir);

        for (String file : metadataFiles) {
            if (!protectedFiles.contains(file)) {
                try {
                    dir.deleteFile(file);
                } catch (IOException e) {
                    logger.warn("Failed to delete metadata file: {} in directory: {}", file, dir, e);
                }
            }
        }
    }
}
