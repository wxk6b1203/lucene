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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Mock S3 client for testing S3Directory and S3LockFactory.
 * Simulates basic S3 operations using in-memory storage.
 */
public class MockS3Client implements S3Client {

  // Bucket -> Key -> Data
  private final Map<String, Map<String, byte[]>> storage = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Long>> lastModified = new ConcurrentHashMap<>();

  public MockS3Client() {
    // Initialize with empty storage
  }

  /**
   * Check if an object exists in the mock S3 storage
   */
  public boolean objectExists(String bucket, String key) {
    Map<String, byte[]> bucketData = storage.get(bucket);
    return bucketData != null && bucketData.containsKey(key);
  }

  /**
   * Put an object into mock S3 storage
   */
  public void putObject(String bucket, String key, byte[] data) {
    storage.computeIfAbsent(bucket, k -> new ConcurrentHashMap<>()).put(key, Arrays.copyOf(data, data.length));
    lastModified.computeIfAbsent(bucket, k -> new ConcurrentHashMap<>()).put(key, System.currentTimeMillis());
  }

  /**
   * Put an object with conditional check (if-none-match: *)
   */
  public void putObjectIfNotExists(String bucket, String key, byte[] data) throws S3Exception {
    Map<String, byte[]> bucketData = storage.computeIfAbsent(bucket, k -> new ConcurrentHashMap<>());

    // Simulate if-none-match: * behavior
    if (bucketData.containsKey(key)) {
      throw S3Exception.builder()
          .statusCode(412)
          .message("Precondition Failed")
          .build();
    }

    bucketData.put(key, Arrays.copyOf(data, data.length));
    lastModified.computeIfAbsent(bucket, k -> new ConcurrentHashMap<>()).put(key, System.currentTimeMillis());
  }

  /**
   * Get object data from mock S3 storage
   */
  public byte[] getObjectData(String bucket, String key) throws S3Exception {
    Map<String, byte[]> bucketData = storage.get(bucket);
    if (bucketData == null || !bucketData.containsKey(key)) {
      throw NoSuchKeyException.builder()
          .message("The specified key does not exist")
          .build();
    }

    byte[] data = bucketData.get(key);
    return Arrays.copyOf(data, data.length);
  }

  /**
   * Get object size
   */
  public long getObjectSize(String bucket, String key) throws S3Exception {
    Map<String, byte[]> bucketData = storage.get(bucket);
    if (bucketData == null || !bucketData.containsKey(key)) {
      throw NoSuchKeyException.builder()
          .message("The specified key does not exist")
          .build();
    }

    return bucketData.get(key).length;
  }

  /**
   * Delete an object from mock S3 storage
   */
  public void deleteObject(String bucket, String key) {
    Map<String, byte[]> bucketData = storage.get(bucket);
    if (bucketData != null) {
      bucketData.remove(key);
      Map<String, Long> bucketLastModified = lastModified.get(bucket);
      if (bucketLastModified != null) {
        bucketLastModified.remove(key);
      }
    }
  }

  /**
   * Copy an object within mock S3 storage
   */
  public void copyObject(String bucket, String sourceKey, String destKey) throws S3Exception {
    byte[] data = getObjectData(bucket, sourceKey);
    putObject(bucket, destKey, data);
  }

  /**
   * List objects with a given prefix
   */
  public String[] listObjects(String bucket, String prefix) {
    Map<String, byte[]> bucketData = storage.get(bucket);
    if (bucketData == null) {
      return new String[0];
    }

    return bucketData.keySet().stream()
        .filter(key -> key.startsWith(prefix))
        .map(key -> key.substring(prefix.length()))
        .filter(name -> !name.isEmpty())
        .sorted()
        .toArray(String[]::new);
  }

  /**
   * Head object operation - check if object exists and get metadata
   */
  public void headObject(String bucket, String key) throws S3Exception {
    if (!objectExists(bucket, key)) {
      throw S3Exception.builder()
          .statusCode(404)
          .message("Not Found")
          .build();
    }
  }

  /**
   * Clear all data (for test cleanup)
   */
  public void clear() {
    storage.clear();
    lastModified.clear();
  }

  /**
   * Get the number of objects in a bucket
   */
  public int getObjectCount(String bucket) {
    Map<String, byte[]> bucketData = storage.get(bucket);
    return bucketData == null ? 0 : bucketData.size();
  }

  @Override
  public String serviceName() {
    return "";
  }

  @Override
  public void close() {

  }
}
