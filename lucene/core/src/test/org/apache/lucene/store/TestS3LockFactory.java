/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Tests S3LockFactory functionality using mock S3 operations.
 */
public class TestS3LockFactory extends LuceneTestCase {

  private MockS3Client mockS3Client;
  private S3Directory directory;
  private S3LockFactory lockFactory;
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_PREFIX = "lucene-test/";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    mockS3Client = new MockS3Client();
    lockFactory = new S3LockFactory();
    directory = new MockS3Directory(mockS3Client, TEST_BUCKET, TEST_PREFIX, lockFactory);
  }

  @After
  public void tearDown() throws Exception {
    if (directory != null) {
      directory.close();
    }
    super.tearDown();
  }

  @Test
  public void testObtainLock() throws IOException {
    String lockName = "test.lock";

    try (Lock lock = lockFactory.obtainLock(directory, lockName)) {
      assertNotNull(lock);

      // Verify lock object exists in S3
      assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName));

      // Verify lock is valid
      lock.ensureValid();
    }

    // After closing, lock object should be deleted
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName));
  }

  @Test
  public void testObtainLockWithWrongDirectoryType() {
    Directory wrongDir = new ByteBuffersDirectory();

    UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> {
      lockFactory.obtainLock(wrongDir, "test.lock");
    });

    assertTrue(e.getMessage().contains("can only be used with S3Directory subclasses"));
  }

  @Test
  public void testDoubleObtainSameLockFails() throws IOException {
    String lockName = "double.lock";

    try (Lock lock1 = lockFactory.obtainLock(directory, lockName)) {
      assertNotNull(lock1);

      // Trying to obtain the same lock should fail
      LockObtainFailedException e = expectThrows(LockObtainFailedException.class, () -> {
        lockFactory.obtainLock(directory, lockName);
      });

      assertTrue(e.getMessage().contains("Lock already held by another process"));
    }
  }

  @Test
  public void testDoubleObtainSameLockInSameJVM() throws IOException {
    String lockName = "jvm.lock";

    try (Lock lock1 = lockFactory.obtainLock(directory, lockName)) {
      assertNotNull(lock1);

      // Simulate same JVM trying to get the same lock again
      LockObtainFailedException e = expectThrows(LockObtainFailedException.class, () -> {
        lockFactory.obtainLock(directory, lockName);
      });

      assertTrue(e.getMessage().contains("Lock already obtained in this JVM"));
    }
  }

  @Test
  public void testLockEnsureValidAfterDelete() throws IOException {
    String lockName = "validate.lock";

    Lock lock = lockFactory.obtainLock(directory, lockName);

    // Manually delete the lock object to simulate external deletion
    mockS3Client.deleteObject(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName);

    // ensureValid should now throw exception
    AlreadyClosedException e = expectThrows(AlreadyClosedException.class, () -> {
      lock.ensureValid();
    });

    assertTrue(e.getMessage().contains("Lock was invalidated"));

    lock.close();
  }

  @Test
  public void testLockEnsureValidAfterClose() throws IOException {
    String lockName = "closed.lock";

    Lock lock = lockFactory.obtainLock(directory, lockName);
    lock.close();

    // ensureValid should throw exception after close
    AlreadyClosedException e = expectThrows(AlreadyClosedException.class, () -> {
      lock.ensureValid();
    });

    assertTrue(e.getMessage().contains("Lock instance already released"));
  }

  @Test
  public void testMultipleLocksWithDifferentNames() throws IOException {
    String lockName1 = "lock1.lock";
    String lockName2 = "lock2.lock";

    try (Lock lock1 = lockFactory.obtainLock(directory, lockName1);
         Lock lock2 = lockFactory.obtainLock(directory, lockName2)) {

      assertNotNull(lock1);
      assertNotNull(lock2);

      // Both locks should be valid
      lock1.ensureValid();
      lock2.ensureValid();

      // Both lock objects should exist in S3
      assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName1));
      assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName2));
    }

    // After closing, both lock objects should be deleted
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName1));
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName2));
  }

  @Test
  public void testConcurrentLockAttempts() throws Exception {
    String lockName = "concurrent.lock";
    ExecutorService executor = Executors.newFixedThreadPool(3);

    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      AtomicReference<Lock> successfulLock = new AtomicReference<>();
      AtomicBoolean[] failed = new AtomicBoolean[3];

      for (int i = 0; i < 3; i++) {
        final int threadIndex = i;
        failed[i] = new AtomicBoolean(false);

        executor.submit(() -> {
          try {
            startLatch.await();
            Lock lock = lockFactory.obtainLock(directory, lockName);
            if (successfulLock.compareAndSet(null, lock)) {
              // This thread got the lock
              Thread.sleep(100); // Hold the lock for a bit
            } else {
              // This shouldn't happen since we expect the other threads to fail
              lock.close();
            }
          } catch (LockObtainFailedException e) {
            failed[threadIndex].set(true);
          } catch (Exception e) {
            // Unexpected exception
            e.printStackTrace();
          }
        });
      }

      // Start all threads
      startLatch.countDown();

      // Wait for completion
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

      // Exactly one thread should have succeeded, others should have failed
      assertNotNull(successfulLock.get());

      int failureCount = 0;
      for (AtomicBoolean failedFlag : failed) {
        if (failedFlag.get()) {
          failureCount++;
        }
      }
      assertEquals(2, failureCount); // Two threads should have failed

      // Clean up
      successfulLock.get().close();

    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testLockWithPathPrefix() throws IOException {
    // Test with different path prefix
    MockS3Directory dirWithPrefix = new MockS3Directory(mockS3Client, TEST_BUCKET, "different/prefix/", lockFactory);
    String lockName = "prefix.lock";

    try (Lock lock = lockFactory.obtainLock(dirWithPrefix, lockName)) {
      assertNotNull(lock);

      // Verify lock object exists with correct prefix
      assertTrue(mockS3Client.objectExists(TEST_BUCKET, "different/prefix/.lock-" + lockName));

      lock.ensureValid();
    }

    // After closing, lock object should be deleted
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, "different/prefix/.lock-" + lockName));

    dirWithPrefix.close();
  }

  @Test
  public void testLockWithEmptyPrefix() throws IOException {
    // Test with empty prefix
    MockS3Directory dirWithoutPrefix = new MockS3Directory(mockS3Client, TEST_BUCKET, "", lockFactory);
    String lockName = "noprefix.lock";

    try (Lock lock = lockFactory.obtainLock(dirWithoutPrefix, lockName)) {
      assertNotNull(lock);

      // Verify lock object exists without prefix
      assertTrue(mockS3Client.objectExists(TEST_BUCKET, ".lock-" + lockName));

      lock.ensureValid();
    }

    // After closing, lock object should be deleted
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, ".lock-" + lockName));

    dirWithoutPrefix.close();
  }

  @Test
  public void testGetDefault() {
    S3LockFactory defaultFactory = S3LockFactory.getDefault();
    assertNotNull(defaultFactory);
    assertTrue(defaultFactory instanceof S3LockFactory);
  }

  @Test
  public void testLockToString() throws IOException {
    String lockName = "tostring.lock";

    try (Lock lock = lockFactory.obtainLock(directory, lockName)) {
      String lockString = lock.toString();
      assertNotNull(lockString);
      assertTrue(lockString.contains("S3Lock"));
      assertTrue(lockString.contains(TEST_BUCKET));
      assertTrue(lockString.contains(".lock-" + lockName));
    }
  }

  @Test
  public void testLockReleaseAfterS3Error() throws IOException {
    String lockName = "error.lock";

    Lock lock = lockFactory.obtainLock(directory, lockName);

    // Simulate S3 error during delete by removing the object first
    mockS3Client.deleteObject(TEST_BUCKET, TEST_PREFIX + ".lock-" + lockName);

    // Close should not throw exception even if S3 delete fails (404)
    lock.close();

    // Verify the lock is marked as closed
    AlreadyClosedException e = expectThrows(AlreadyClosedException.class, () -> {
      lock.ensureValid();
    });
    assertTrue(e.getMessage().contains("Lock instance already released"));
  }

  /**
   * Mock S3Directory that extends the original for testing with injected mock client
   */
  private static class MockS3Directory extends S3Directory {
    private final MockS3Client mockClient;
    private final String bucket;
    private final String pathPrefix;

    public MockS3Directory(MockS3Client mockClient, String bucket, String pathPrefix, LockFactory lockFactory) throws IOException {
      super(lockFactory, new S3Config(), "", "");
      this.mockClient = mockClient;
      this.bucket = bucket;
      this.pathPrefix = pathPrefix == null ? "" : pathPrefix;
    }

    // Expose package-private methods for S3LockFactory access
    public S3Client getS3Client() {
      return mockClient;
    }

    public String getBucket() {
      return bucket;
    }

    public String getPathPrefix() {
      return pathPrefix;
    }

    // Implement required abstract methods (minimal implementation for lock testing)
    @Override
    public String[] listAll() throws IOException {
      return new String[0];
    }

    @Override
    public void deleteFile(String name) throws IOException {
      mockClient.deleteObject(bucket, pathPrefix + name);
    }

    @Override
    public long fileLength(String name) throws IOException {
      return mockClient.getObjectSize(bucket, pathPrefix + name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      return new MockS3IndexOutput(name, mockClient, bucket, pathPrefix + name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
      String tempName = prefix + "_" + System.nanoTime() + "_" + suffix;
      return createOutput(tempName, context);
    }

    @Override
    public void sync(java.util.Collection<String> names) throws IOException {
      // No-op
    }

    @Override
    public void syncMetaData() throws IOException {
      // No-op
    }

    @Override
    public void rename(String source, String dest) throws IOException {
      mockClient.copyObject(bucket, pathPrefix + source, pathPrefix + dest);
      mockClient.deleteObject(bucket, pathPrefix + source);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new MockS3IndexInput(name, mockClient, bucket, pathPrefix + name);
    }

    @Override
    public void close() throws IOException {
      // No-op
    }

    @Override
    public java.util.Set<String> getPendingDeletions() throws IOException {
      return java.util.Collections.emptySet();
    }
  }
}
