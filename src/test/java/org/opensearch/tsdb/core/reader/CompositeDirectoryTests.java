/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Unit tests for CompositeDirectory functionality.
 */
public class CompositeDirectoryTests extends OpenSearchTestCase {

    private TestDirectoryHelper helper;
    private CompositeDirectory compositeDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        helper = new TestDirectoryHelper();
        compositeDirectory = new CompositeDirectory(helper.getAllDirectories());
    }

    @Override
    public void tearDown() throws Exception {
        if (compositeDirectory != null) {
            compositeDirectory.close();
        }
        if (helper != null) {
            helper.close();
        }
        super.tearDown();
    }

    public void testListAllCombinesFilesFromAllDirectories() throws IOException {
        String[] allFiles = compositeDirectory.listAll();

        assertEquals("Should have all files from both directories", 4, allFiles.length);
        Set<String> fileSet = Set.of(allFiles);
        assertTrue("Should contain file1.txt", fileSet.contains("file1.txt"));
        assertTrue("Should contain file2.txt", fileSet.contains("file2.txt"));
        assertTrue("Should contain file3.txt", fileSet.contains("file3.txt"));
        assertTrue("Should contain common.txt", fileSet.contains("common.txt"));
    }

    public void testFileLengthThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.fileLength("file1.txt");
        });
        assertTrue("Exception message should mention duplicate file names", exception.getMessage().contains("duplicate in file names"));
    }

    public void testOpenInputThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.openInput("file1.txt", IOContext.DEFAULT);
        });
        assertTrue("Exception message should mention duplicate file names", exception.getMessage().contains("duplicate in file names"));
    }

    public void testDeleteFileThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.deleteFile("file1.txt");
        });
        assertTrue("Exception message should mention duplicate file names", exception.getMessage().contains("duplicate in file names"));
    }

    public void testRenameThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.rename("file1.txt", "renamed_file1.txt");
        });
        assertTrue("Exception message should mention duplicate file names", exception.getMessage().contains("duplicate in file names"));
    }

    public void testSyncCallsAllDirectories() throws IOException {
        Collection<String> filesToSync = Arrays.asList("file1.txt", "file2.txt");

        // This should not throw an exception if all directories support sync
        compositeDirectory.sync(filesToSync);

        // Verify by checking the helper's tracking
        assertTrue("Sync should have been called on directory1", helper.isSyncCalled(0));
        assertTrue("Sync should have been called on directory2", helper.isSyncCalled(1));
    }

    public void testSyncFailsIfAnyDirectoryFails() throws IOException {
        helper.setShouldFailSync(0, true);

        Collection<String> filesToSync = Arrays.asList("file1.txt");

        IOException exception = expectThrows(IOException.class, () -> { compositeDirectory.sync(filesToSync); });
        assertNotNull("Exception should be thrown", exception);
    }

    public void testSyncMetaDataCallsAllDirectories() throws IOException {
        compositeDirectory.syncMetaData();

        assertTrue("SyncMetaData should have been called on directory1", helper.isSyncMetaDataCalled(0));
        assertTrue("SyncMetaData should have been called on directory2", helper.isSyncMetaDataCalled(1));
    }

    public void testSyncMetaDataFailsIfAnyDirectoryFails() throws IOException {
        helper.setShouldFailSyncMetaData(1, true);

        IOException exception = expectThrows(IOException.class, () -> { compositeDirectory.syncMetaData(); });
        assertNotNull("Exception should be thrown", exception);
    }

    public void testObtainLockAcquiresFromAllDirectories() throws IOException {
        try (Lock lock = compositeDirectory.obtainLock("test.lock")) {
            assertNotNull("Lock should be acquired", lock);
            // Verify the lock can be used without throwing exceptions
            lock.ensureValid();
        }
        // Test passes if no exceptions are thrown
    }

    public void testObtainLockFailsIfAnyDirectoryFails() throws IOException {
        helper.setShouldFailLock(1, true);

        IOException exception = expectThrows(IOException.class, () -> { compositeDirectory.obtainLock("test.lock"); });
        assertNotNull("Exception should be thrown", exception);
        assertTrue("Should contain failure message", exception.getMessage().contains("Forced lock failure"));
    }

    public void testObtainLockCleanupWithSuppressedExceptions() throws IOException {
        // Set directory1 to fail during lock close (cleanup)
        helper.setShouldFailLockClose(0, true);
        // Set directory2 to fail during lock acquisition (main failure)
        helper.setShouldFailLock(1, true);

        IOException exception = expectThrows(IOException.class, () -> { compositeDirectory.obtainLock("test.lock"); });
        assertNotNull("Exception should be thrown", exception);
        assertTrue("Should contain lock failure message", exception.getMessage().contains("Forced lock failure"));

        // Verify that cleanup exception is added as suppressed
        assertEquals("Should have one suppressed exception", 1, exception.getSuppressed().length);
        assertTrue(
            "Suppressed exception should contain lock close failure message",
            exception.getSuppressed()[0].getMessage().contains("Forced lock close failure")
        );

        // Reset the failure flags
        helper.setShouldFailLockClose(0, false);
        helper.setShouldFailLock(1, false);
    }

    public void testGetPendingDeletionsCombinesFromAllDirectories() throws IOException {
        helper.addPendingDeletion(0, "pending1.txt");
        helper.addPendingDeletion(1, "pending2.txt");
        helper.addPendingDeletion(1, "pending3.txt");

        Set<String> pendingDeletions = compositeDirectory.getPendingDeletions();

        assertEquals("Should have 3 pending deletions", 3, pendingDeletions.size());
        assertTrue("Should contain pending1.txt", pendingDeletions.contains("pending1.txt"));
        assertTrue("Should contain pending2.txt", pendingDeletions.contains("pending2.txt"));
        assertTrue("Should contain pending3.txt", pendingDeletions.contains("pending3.txt"));
    }

    public void testUnsupportedOperationsThrowExceptions() {
        UnsupportedOperationException exception1 = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.createOutput("test.txt", IOContext.DEFAULT);
        });
        assertNotNull("createOutput should throw UnsupportedOperationException", exception1);

        UnsupportedOperationException exception2 = expectThrows(UnsupportedOperationException.class, () -> {
            compositeDirectory.createTempOutput("test", "tmp", IOContext.DEFAULT);
        });
        assertNotNull("createTempOutput should throw UnsupportedOperationException", exception2);
    }

    public void testToStringIncludesDirectories() {
        String toString = compositeDirectory.toString();
        assertTrue("toString should contain CompositeDirectory", toString.contains("CompositeDirectory"));
        assertTrue("toString should contain directory info", toString.contains("TestDirectory") || toString.contains("Directory"));
    }

    public void testCloseClosesAllDirectories() throws IOException {
        compositeDirectory.close();

        assertTrue("Directory1 should be closed", helper.isClosed(0));
        assertTrue("Directory2 should be closed", helper.isClosed(1));
    }

    public void testCloseWithSuppressedExceptions() throws IOException {
        helper.setShouldFailClose(0, true);
        helper.setShouldFailClose(1, true);

        // Create a new composite directory to test (separate from the one used in tearDown)
        CompositeDirectory testDirectory = new CompositeDirectory(helper.getAllDirectories());

        IOException exception = expectThrows(IOException.class, () -> { testDirectory.close(); });
        assertNotNull("Exception should be thrown", exception);
        assertEquals("Should have one suppressed exception", 1, exception.getSuppressed().length);
        assertTrue(
            "First exception message should contain 'Forced close failure'",
            exception.getMessage().contains("Forced close failure")
        );
        assertTrue(
            "Suppressed exception message should contain 'Forced close failure'",
            exception.getSuppressed()[0].getMessage().contains("Forced close failure")
        );

        // Reset the failure flags to prevent tearDown() issues
        helper.setShouldFailClose(0, false);
        helper.setShouldFailClose(1, false);
    }

    public void testLockCloseWithSuppressedExceptions() throws IOException {
        helper.setShouldFailLockClose(0, true);
        helper.setShouldFailLockClose(1, true);

        Lock lock = compositeDirectory.obtainLock("test.lock");
        IOException exception = expectThrows(IOException.class, lock::close);
        assertNotNull("Exception should be thrown", exception);
        assertEquals("Should have one suppressed exception", 1, exception.getSuppressed().length);
        assertTrue(
            "First exception message should contain 'Forced lock close failure'",
            exception.getMessage().contains("Forced lock close failure")
        );
        assertTrue(
            "Suppressed exception message should contain 'Forced lock close failure'",
            exception.getSuppressed()[0].getMessage().contains("Forced lock close failure")
        );

        // Reset the failure flags
        helper.setShouldFailLockClose(0, false);
        helper.setShouldFailLockClose(1, false);
    }

    public void testEmptyDirectoryList() throws IOException {
        try (CompositeDirectory emptyComposite = new CompositeDirectory(List.of())) {
            String[] files = emptyComposite.listAll();
            assertEquals("Empty composite should have no files", 0, files.length);

            Set<String> pendingDeletions = emptyComposite.getPendingDeletions();
            assertTrue("Empty composite should have no pending deletions", pendingDeletions.isEmpty());
        }
    }

    /**
     * Helper class to create and manage test directories with controlled behavior.
     */
    private static class TestDirectoryHelper {
        private final TestDirectory directory1;
        private final TestDirectory directory2;

        public TestDirectoryHelper() throws IOException {
            directory1 = new TestDirectory();
            directory2 = new TestDirectory();

            // Setup directory1 with files
            directory1.addFile("file1.txt", 100L);
            directory1.addFile("common.txt", 150L);

            // Setup directory2 with files
            directory2.addFile("file2.txt", 200L);
            directory2.addFile("file3.txt", 300L);
            directory2.addFile("common.txt", 250L); // Different size, but first one wins
        }

        public List<Directory> getAllDirectories() {
            return Arrays.asList(directory1, directory2);
        }

        public boolean isSyncCalled(int directoryIndex) {
            return getDirectory(directoryIndex).isSyncCalled();
        }

        public boolean isSyncMetaDataCalled(int directoryIndex) {
            return getDirectory(directoryIndex).isSyncMetaDataCalled();
        }

        public boolean isClosed(int directoryIndex) {
            return getDirectory(directoryIndex).isClosed();
        }

        public void setShouldFailSync(int directoryIndex, boolean shouldFail) {
            getDirectory(directoryIndex).setShouldFailSync(shouldFail);
        }

        public void setShouldFailSyncMetaData(int directoryIndex, boolean shouldFail) {
            getDirectory(directoryIndex).setShouldFailSyncMetaData(shouldFail);
        }

        public void setShouldFailLock(int directoryIndex, boolean shouldFail) {
            getDirectory(directoryIndex).setShouldFailLock(shouldFail);
        }

        public void setShouldFailClose(int directoryIndex, boolean shouldFail) {
            getDirectory(directoryIndex).setShouldFailClose(shouldFail);
        }

        public void setShouldFailLockClose(int directoryIndex, boolean shouldFail) {
            getDirectory(directoryIndex).setShouldFailLockClose(shouldFail);
        }

        public void addPendingDeletion(int directoryIndex, String fileName) {
            getDirectory(directoryIndex).addToPendingDeletions(fileName);
        }

        private TestDirectory getDirectory(int index) {
            return index == 0 ? directory1 : directory2;
        }

        public void close() throws IOException {
            if (directory1 != null) directory1.close();
            if (directory2 != null) directory2.close();
        }
    }

    /**
     * Test implementation of Directory that tracks method calls and allows controlled failures.
     */
    private static class TestDirectory extends Directory {
        private final Directory delegate;
        private final Set<String> pendingDeletions = new java.util.LinkedHashSet<>();
        private boolean syncCalled = false;
        private boolean syncMetaDataCalled = false;
        private boolean shouldFailSync = false;
        private boolean shouldFailSyncMetaData = false;
        private boolean shouldFailLock = false;
        private boolean shouldFailClose = false;
        private boolean shouldFailLockClose = false;
        private boolean closed = false;

        public TestDirectory() {
            this.delegate = new ByteBuffersDirectory();
        }

        public void addFile(String name, long length) throws IOException {
            try (IndexOutput output = delegate.createOutput(name, IOContext.DEFAULT)) {
                // Write some dummy data to achieve the desired length
                byte[] data = new byte[(int) length];
                output.writeBytes(data, 0, data.length);
            }
        }

        public void addToPendingDeletions(String fileName) {
            pendingDeletions.add(fileName);
        }

        @Override
        public String[] listAll() throws IOException {
            return delegate.listAll();
        }

        @Override
        public void deleteFile(String name) throws IOException {
            delegate.deleteFile(name);
        }

        @Override
        public long fileLength(String name) throws IOException {
            return delegate.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return delegate.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return delegate.createTempOutput(prefix, suffix, context);
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            syncCalled = true;
            if (shouldFailSync) {
                throw new IOException("Forced sync failure");
            }
            delegate.sync(names);
        }

        @Override
        public void syncMetaData() throws IOException {
            syncMetaDataCalled = true;
            if (shouldFailSyncMetaData) {
                throw new IOException("Forced syncMetaData failure");
            }
            delegate.syncMetaData();
        }

        @Override
        public void rename(String source, String dest) throws IOException {
            delegate.rename(source, dest);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return delegate.openInput(name, context);
        }

        @Override
        public Lock obtainLock(String name) throws IOException {
            if (shouldFailLock) {
                throw new IOException("Forced lock failure");
            }
            return new TestLock(delegate.obtainLock(name), this);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (shouldFailClose) {
                throw new IOException("Forced close failure");
            }
            delegate.close();
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            Set<String> result = new java.util.LinkedHashSet<>(delegate.getPendingDeletions());
            result.addAll(pendingDeletions);
            return result;
        }

        public boolean isSyncCalled() {
            return syncCalled;
        }

        public boolean isSyncMetaDataCalled() {
            return syncMetaDataCalled;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setShouldFailSync(boolean shouldFail) {
            this.shouldFailSync = shouldFail;
        }

        public void setShouldFailSyncMetaData(boolean shouldFail) {
            this.shouldFailSyncMetaData = shouldFail;
        }

        public void setShouldFailLock(boolean shouldFail) {
            this.shouldFailLock = shouldFail;
        }

        public void setShouldFailClose(boolean shouldFail) {
            this.shouldFailClose = shouldFail;
        }

        public void setShouldFailLockClose(boolean shouldFail) {
            this.shouldFailLockClose = shouldFail;
        }
    }

    /**
     * Test lock implementation that tracks acquisition state.
     */
    private static class TestLock extends Lock {
        private final Lock delegate;
        private final TestDirectory parent;

        public TestLock(Lock delegate, TestDirectory parent) {
            this.delegate = delegate;
            this.parent = parent;
        }

        @Override
        public void close() throws IOException {
            if (parent.shouldFailLockClose) {
                throw new IOException("Forced lock close failure");
            }
            delegate.close();
        }

        @Override
        public void ensureValid() throws IOException {
            delegate.ensureValid();
        }

    }
}
