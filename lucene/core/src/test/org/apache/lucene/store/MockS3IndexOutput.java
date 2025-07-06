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

/**
 * Mock IndexOutput implementation for S3Directory testing.
 * Writes data to a MockS3Client instead of real S3.
 */
public class MockS3IndexOutput extends IndexOutput {

  private final MockS3Client s3Client;
  private final String bucket;
  private final String key;
  private final ByteArrayOutputStream buffer;
  private boolean closed = false;

  public MockS3IndexOutput(String resourceDescription, MockS3Client s3Client, String bucket, String key) {
    super(resourceDescription, resourceDescription);
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.key = key;
    this.buffer = new ByteArrayOutputStream();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      // Upload the buffered data to mock S3
      byte[] data = buffer.toByteArray();
      s3Client.putObject(bucket, key, data);
    } finally {
      closed = true;
      buffer.close();
    }
  }

  @Override
  public long getFilePointer() {
    return buffer.size();
  }

  @Override
  public long getChecksum() throws IOException {
    // Simple checksum implementation for testing
    byte[] data = buffer.toByteArray();
    long checksum = 0;
    for (byte b : data) {
      checksum = checksum * 31 + (b & 0xFF);
    }
    return checksum;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    ensureOpen();
    buffer.write(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    ensureOpen();
    buffer.write(b, offset, length);
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new AlreadyClosedException("IndexOutput is closed");
    }
  }
}
