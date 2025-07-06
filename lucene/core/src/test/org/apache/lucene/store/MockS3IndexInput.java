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

import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Mock IndexInput implementation for S3Directory testing.
 * Reads data from a MockS3Client instead of real S3.
 */
public class MockS3IndexInput extends IndexInput {

  private final MockS3Client s3Client;
  private final String bucket;
  private final String key;
  private final byte[] data;
  private final long length;
  private long position = 0;
  private boolean closed = false;

  public MockS3IndexInput(String resourceDescription, MockS3Client s3Client, String bucket, String key) throws IOException {
    super(resourceDescription);
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.key = key;

    try {
      this.data = s3Client.getObjectData(bucket, key);
      this.length = data.length;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new NoSuchFileException("File not found: " + key);
      }
      throw new IOException("Failed to read from S3: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

  @Override
  public long getFilePointer() {
    return position;
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    if (pos < 0 || pos > length) {
      throw new IOException("Invalid seek position: " + pos);
    }
    this.position = pos;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    ensureOpen();
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException("Invalid slice parameters");
    }

    return new MockS3IndexInputSlice(getFullSliceDescription(sliceDescription), data, offset, length);
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    if (position >= length) {
      throw new IOException("Read past EOF");
    }
    return data[(int) position++];
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    if (position + len > length) {
      throw new IOException("Read past EOF");
    }
    System.arraycopy(data, (int) position, b, offset, len);
    position += len;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new AlreadyClosedException("IndexInput is closed");
    }
  }

  @Override
  public IndexInput clone() {
    MockS3IndexInput clone = (MockS3IndexInput) super.clone();
    clone.position = 0; // Reset position for clone
    return clone;
  }

  /**
   * Simple slice implementation for testing
   */
  private static class MockS3IndexInputSlice extends IndexInput {
    private final byte[] data;
    private final long offset;
    private final long length;
    private long position = 0;
    private boolean closed = false;

    public MockS3IndexInputSlice(String resourceDescription, byte[] data, long offset, long length) {
      super(resourceDescription);
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public long getFilePointer() {
      return position;
    }

    @Override
    public void seek(long pos) throws IOException {
      ensureOpen();
      if (pos < 0 || pos > length) {
        throw new IOException("Invalid seek position: " + pos);
      }
      this.position = pos;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      if (offset < 0 || length < 0 || offset + length > this.length) {
        throw new IllegalArgumentException("Invalid slice parameters");
      }
      return new MockS3IndexInputSlice(getFullSliceDescription(sliceDescription),
                                       data, this.offset + offset, length);
    }

    @Override
    public byte readByte() throws IOException {
      ensureOpen();
      if (position >= length) {
        throw new IOException("Read past EOF");
      }
      return data[(int) (offset + position++)];
    }

    @Override
    public void readBytes(byte[] b, int off, int len) throws IOException {
      ensureOpen();
      if (position + len > length) {
        throw new IOException("Read past EOF");
      }
      System.arraycopy(data, (int) (offset + position), b, off, len);
      position += len;
    }

    private void ensureOpen() throws IOException {
      if (closed) {
        throw new AlreadyClosedException("IndexInput is closed");
      }
    }

    @Override
    public IndexInput clone() {
      MockS3IndexInputSlice clone = (MockS3IndexInputSlice) super.clone();
      clone.position = 0; // Reset position for clone
      return clone;
    }
  }
}
