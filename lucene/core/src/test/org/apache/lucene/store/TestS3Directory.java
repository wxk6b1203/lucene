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
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests S3Directory functionality using mock S3 operations.
 */
public class TestS3Directory extends LuceneTestCase {

  private MockS3Client mockS3Client;
  private S3Directory directory;
  private static final String TEST_BUCKET = "lucene";
  private static final String TEST_PREFIX = "/";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    mockS3Client = new MockS3Client();

    S3Directory.S3Config s3Config = new S3Directory.S3Config();
    s3Config.pathStyleAccess(true);
    s3Config.accessKey("m354iFZwVQKXA1u8TIoH");
    s3Config.secretKey("zbUb6jBMAcdVkQyJdCcYxk9IxplkDeUiSK0mDd5b");
    s3Config.endpoint("http://10.0.10.51:9000");
    // 使用 Mock S3Directory，避免网络连接
    directory = new S3Directory(S3LockFactory.getDefault(), s3Config, TEST_BUCKET, TEST_PREFIX);
  }

  @After
  public void tearDown() throws Exception {
    if (directory != null) {
      directory.close();
    }
    super.tearDown();
  }

  @Test
  public void testListAllEmpty() throws IOException {
    String[] files = directory.listAll();
    assertEquals(0, files.length);
  }

  @Test
  public void testCreateOutput() throws IOException {
    String fileName = "test-file.txt";
    byte[] data = "Hello World".getBytes();

    try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + fileName));
    assertArrayEquals(data, mockS3Client.getObjectData(TEST_BUCKET, TEST_PREFIX + fileName));
  }

  @Test
  public void testOpenInput() throws IOException {
    String fileName = "test-input.txt";
    byte[] data = "Test data for reading".getBytes();

    // Put data directly into mock S3
    mockS3Client.putObject(TEST_BUCKET, TEST_PREFIX + fileName, data);

    try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
      assertEquals(data.length, input.length());

      byte[] readData = new byte[data.length];
      input.readBytes(readData, 0, data.length);
      assertArrayEquals(data, readData);
    }
  }

  @Test
  public void testDeleteFile() throws IOException {
    String fileName = "test-delete.txt";
    byte[] data = "Data to be deleted".getBytes();

    // Create file
    try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + fileName));

    // Delete file
    directory.deleteFile(fileName);

    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + fileName));
  }

  @Test
  public void testDeleteNonExistentFile() {
    expectThrows(NoSuchFileException.class, () -> directory.deleteFile("non-existent-file.txt"));
  }

  @Test
  public void testFileLength() throws IOException {
    String fileName = "test-length.txt";
    byte[] data = TestUtil.randomSimpleString(random(), 100, 1000).getBytes();

    try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertEquals(data.length, directory.fileLength(fileName));
  }

  @Test
  public void testFileLengthNonExistent() {
    expectThrows(NoSuchFileException.class, () -> directory.fileLength("non-existent.txt"));
  }

  @Test
  public void testListAllWithFiles() throws IOException {
    String[] fileNames = {"file1.txt", "file2.dat", "file3.bin"};

    for (String fileName : fileNames) {
      try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
        output.writeString("content");
      }
    }

    String[] listedFiles = directory.listAll();
    assertEquals(fileNames.length, listedFiles.length);

    Set<String> expectedFiles = Set.of(fileNames);
    Set<String> actualFiles = Set.of(listedFiles);
    assertEquals(expectedFiles, actualFiles);
  }

  @Test
  public void testRename() throws IOException {
    String sourceName = "source.txt";
    String destName = "destination.txt";
    byte[] data = "Rename test data".getBytes();

    // Create source file
    try (IndexOutput output = directory.createOutput(sourceName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + sourceName));
    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + destName));

    // Rename
    directory.rename(sourceName, destName);

    assertFalse(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + sourceName));
    assertTrue(mockS3Client.objectExists(TEST_BUCKET, TEST_PREFIX + destName));

    // Verify content is preserved
    assertArrayEquals(data, mockS3Client.getObjectData(TEST_BUCKET, TEST_PREFIX + destName));
  }

  @Test
  public void testSync() throws IOException {
    String[] fileNames = {"sync1.txt", "sync2.txt"};

    for (String fileName : fileNames) {
      try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
        output.writeString("sync test");
      }
    }

    // Sync should not throw exception
    directory.sync(Set.of(fileNames));
  }

  @Test
  public void testCreateTempOutput() throws IOException {
    try (IndexOutput output = directory.createTempOutput("prefix", "suffix", IOContext.DEFAULT)) {
      output.writeString("temp data");
      // The temp file name should be auto-generated
      assertNotNull(output.toString());
    }
  }

  @Test
  public void testGetPendingDeletions() throws IOException {
    Set<String> pending = directory.getPendingDeletions();
    assertNotNull(pending);
    // Should be empty initially
    assertEquals(0, pending.size());
  }

  @Test
  public void testLargeFile() throws IOException {
    String fileName = "large-file.dat";
    int size = 1024 * 1024; // 1MB
    byte[] data = new byte[size];
    random().nextBytes(data);

    try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertEquals(size, directory.fileLength(fileName));

    try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
      assertEquals(size, input.length());

      byte[] readData = new byte[size];
      input.readBytes(readData, 0, size);
      assertArrayEquals(data, readData);
    }
  }

  /**
   * Mock S3Directory for testing purposes
   */
  private static class MockS3DirectoryForTest extends BaseDirectory {
    private final MockS3Client mockClient;
    private final String bucket;
    private final String pathPrefix;

    public MockS3DirectoryForTest(MockS3Client mockClient, String bucket, String pathPrefix) throws IOException {
      super(new S3LockFactory());
      this.mockClient = mockClient;
      this.bucket = bucket;
      this.pathPrefix = pathPrefix == null ? "" : pathPrefix;
    }

    @Override
    public String[] listAll() {
      return mockClient.listObjects(bucket, pathPrefix);
    }

    @Override
    public void deleteFile(String name) throws IOException {
      String fullKey = pathPrefix + name;
      if (!mockClient.objectExists(bucket, fullKey)) {
        throw new NoSuchFileException("File not found: " + name);
      }
      mockClient.deleteObject(bucket, fullKey);
    }

    @Override
    public long fileLength(String name) throws IOException {
      String fullKey = pathPrefix + name;
      if (!mockClient.objectExists(bucket, fullKey)) {
        throw new NoSuchFileException("File not found: " + name);
      }
      return mockClient.getObjectSize(bucket, fullKey);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
      String fullKey = pathPrefix + name;
      return new MockS3IndexOutput(name, mockClient, bucket, fullKey);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
      String tempName = prefix + "_" + System.nanoTime() + "_" + suffix;
      return createOutput(tempName, context);
    }

    @Override
    public void sync(Collection<String> names) {
      // No-op for mock
    }

    @Override
    public void syncMetaData() {
      // No-op for mock
    }

    @Override
    public void rename(String source, String dest) throws IOException {
      String sourceKey = pathPrefix + source;
      String destKey = pathPrefix + dest;

      if (!mockClient.objectExists(bucket, sourceKey)) {
        throw new NoSuchFileException("Source file not found: " + source);
      }

      mockClient.copyObject(bucket, sourceKey, destKey);
      mockClient.deleteObject(bucket, sourceKey);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      String fullKey = pathPrefix + name;
      if (!mockClient.objectExists(bucket, fullKey)) {
        throw new NoSuchFileException("File not found: " + name);
      }
      return new MockS3IndexInput(name, mockClient, bucket, fullKey);
    }

    @Override
    public void close() {
      // No-op for mock
    }

    @Override
    public Set<String> getPendingDeletions() {
      return new HashSet<>();
    }
  }
}
