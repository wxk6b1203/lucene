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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import static org.junit.Assert.*;

/**
 * Integration tests for S3Directory with real S3 connections.
 * These tests are ignored by default to avoid network dependencies in CI.
 * Run manually when testing against real S3 services.
 */
@Ignore("Integration tests require real S3 service - run manually")
public class S3DirectoryIntegrationTest {

  private S3Directory directory;
  private static final String TEST_BUCKET = "lucene-test";
  private static final String TEST_PREFIX = "test/";

  @Before
  public void setUp() throws Exception {
    // 只有在手动运行集成测试时才连接真实S3
    S3Directory.S3Config config = new S3Directory.S3Config()
            .accessKey("your-access-key")
            .secretKey("your-secret-key")
            .endpoint("http://10.0.10.51:9000")
            .pathStyleAccess(true);

    directory = S3Directory.open(config, TEST_BUCKET, TEST_PREFIX);
  }

  @After
  public void tearDown() throws Exception {
    if (directory != null) {
      // 清理测试文件
      try {
        String[] files = directory.listAll();
        for (String file : files) {
          directory.deleteFile(file);
        }
      } catch (Exception e) {
        // 忽略清理错误
      }
      directory.close();
    }
  }

  @Test
  public void testRealS3Connection() throws IOException {
    // 测试真实S3连接
    String fileName = "integration-test.txt";
    byte[] data = "Integration test data".getBytes();

    try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
      output.writeBytes(data, data.length);
    }

    assertEquals(data.length, directory.fileLength(fileName));

    try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
      byte[] readData = new byte[data.length];
      input.readBytes(readData, 0, data.length);
      assertArrayEquals(data, readData);
    }

    directory.deleteFile(fileName);
  }
}
