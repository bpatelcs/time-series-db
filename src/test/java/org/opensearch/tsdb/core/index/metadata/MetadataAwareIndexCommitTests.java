/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetadataAwareIndexCommitTests extends OpenSearchTestCase {

    public void testGetFileNamesIncludesMetadataFile() throws IOException {
        // Create a mock IndexCommit
        IndexCommit mockCommit = new MockIndexCommit(Set.of("_0.cfs", "_0.si", "segments_1"));

        String metadataFilename = "series_metadata_1";
        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(mockCommit, metadataFilename);

        Collection<String> files = wrappedCommit.getFileNames();
        assertEquals(4, files.size());
        assertTrue(files.contains("_0.cfs"));
        assertTrue(files.contains("_0.si"));
        assertTrue(files.contains("segments_1"));
        assertTrue(files.contains("series_metadata_1"));
    }

    public void testGetFileNamesWithoutMetadataFile() throws IOException {
        // Create a mock IndexCommit
        IndexCommit mockCommit = new MockIndexCommit(Set.of("_0.cfs", "_0.si", "segments_1"));

        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(mockCommit, null);

        Collection<String> files = wrappedCommit.getFileNames();
        assertEquals(3, files.size());
        assertTrue(files.contains("_0.cfs"));
        assertTrue(files.contains("_0.si"));
        assertTrue(files.contains("segments_1"));
    }

    public void testGetMetadataFilename() {
        IndexCommit mockCommit = new MockIndexCommit(Set.of());

        String metadataFilename = "series_metadata_16"; // 42 in base-36 is "16"
        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(mockCommit, metadataFilename);

        assertEquals(metadataFilename, wrappedCommit.getMetadataFilename());
    }

    public void testGetDelegate() {
        IndexCommit mockCommit = new MockIndexCommit(Set.of());

        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(mockCommit, "metadata_file");

        assertSame(mockCommit, wrappedCommit.getDelegate());
    }

    public void testDelegatedMethods() throws IOException {
        IndexCommit mockCommit = new MockIndexCommit(Set.of("file1"));
        mockCommit.getUserData().put("key", "value");

        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(mockCommit, null);

        assertEquals("segments_1", wrappedCommit.getSegmentsFileName());
        assertEquals(1L, wrappedCommit.getGeneration());
        assertEquals(1, wrappedCommit.getSegmentCount());
        assertEquals("value", wrappedCommit.getUserData().get("key"));
        assertFalse(wrappedCommit.isDeleted());
    }

    /**
     * Mock IndexCommit for testing
     */
    private static class MockIndexCommit extends IndexCommit {
        private final Set<String> files;
        private final Map<String, String> userData = new HashMap<>();
        private boolean deleted = false;

        MockIndexCommit(Set<String> files) {
            this.files = files;
        }

        @Override
        public String getSegmentsFileName() {
            return "segments_1";
        }

        @Override
        public Collection<String> getFileNames() {
            return files;
        }

        @Override
        public Directory getDirectory() {
            return null;
        }

        @Override
        public void delete() {
            deleted = true;
        }

        @Override
        public boolean isDeleted() {
            return deleted;
        }

        @Override
        public int getSegmentCount() {
            return 1;
        }

        @Override
        public long getGeneration() {
            return 1L;
        }

        @Override
        public Map<String, String> getUserData() {
            return userData;
        }
    }
}
