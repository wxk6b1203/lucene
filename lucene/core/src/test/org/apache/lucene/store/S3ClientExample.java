package org.apache.lucene.store;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class S3ClientExample {

    public static void main(String[] args) {
        // 1. 创建S3Client实例
        Region region = Region.US_EAST_1; // 替换为你的区域
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
                "m354iFZwVQKXA1u8TIoH",
                "zbUb6jBMAcdVkQyJdCcYxk9IxplkDeUiSK0mDd5b"
        );
        S3Client s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .region(region)
                .endpointOverride(URI.create("http://10.0.10.51:9000")) // 替换为你的S3端点
                .serviceConfiguration(b -> b.pathStyleAccessEnabled(true)) // 启用路径样式访问
                .build();

        // 2. 配置上传请求
        String bucketName = "lucene"; // 替换为你的桶名
        String key = "/your-object-key.txt";     // 存储对象的键（路径）

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try {
            // 3. 执行上传操作
            PutObjectResponse response = s3Client.putObject(
                    putObjectRequest,
                    RequestBody.fromBytes("GG?".getBytes(StandardCharsets.UTF_8))
            );

            System.out.println("上传成功，ETag: " + response.eTag());
        } catch (SdkException e) {
            System.err.println("上传失败: " + e.getMessage());
        }
    }
}
