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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * Wrapper around Lucene's IndexCommit that includes live series metadata file
 * in the list of files returned by getFileNames().
 *
 * This ensures that metadata files are properly included in snapshots and recovery operations.
 */
public class MetadataAwareIndexCommit extends IndexCommit {

    private final IndexCommit delegate;
    private final String metadataFilename;

    /**
     * Create a new MetadataAwareIndexCommit.
     *
     * @param delegate the underlying Lucene IndexCommit
     * @param metadataFilename the filename of the metadata file, or null if no metadata
     */
    public MetadataAwareIndexCommit(IndexCommit delegate, String metadataFilename) {
        this.delegate = delegate;
        this.metadataFilename = metadataFilename;
    }

    /**
     * Get the metadata filename associated with this commit.
     *
     * @return the metadata filename, or null if no metadata
     */
    public String getMetadataFilename() {
        return metadataFilename;
    }

    /**
     * Get the underlying Lucene IndexCommit.
     *
     * @return the delegate IndexCommit
     */
    public IndexCommit getDelegate() {
        return delegate;
    }

    @Override
    public String getSegmentsFileName() {
        return delegate.getSegmentsFileName();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
        Collection<String> files = new HashSet<>(delegate.getFileNames());
        if (metadataFilename != null) {
            files.add(metadataFilename);
        }
        return files;
    }

    @Override
    public Directory getDirectory() {
        return delegate.getDirectory();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public boolean isDeleted() {
        return delegate.isDeleted();
    }

    @Override
    public int getSegmentCount() {
        return delegate.getSegmentCount();
    }

    @Override
    public long getGeneration() {
        return delegate.getGeneration();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
        return delegate.getUserData();
    }
}
