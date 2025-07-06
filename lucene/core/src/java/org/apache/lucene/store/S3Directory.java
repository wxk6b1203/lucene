package org.apache.lucene.store;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class S3Directory extends BaseDirectory {
    private final S3Client s3Client;
    private final String bucket;
    private final String pathPrefix;

    private final Set<String> pendingDeletes = ConcurrentHashMap.newKeySet();

    /**
     * Configuration builder for S3Directory
     */
    public static class S3Config {
        private String accessKey;
        private String secretKey;
        private String endpoint;
        private Region region = Region.US_EAST_1;
        private boolean pathStyleAccess = false;

        public S3Config accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public S3Config secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public S3Config endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public S3Config region(String region) {
            this.region = Region.of(region);
            return this;
        }

        public S3Config pathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public Region getRegion() {
            return region;
        }

        public boolean isPathStyleAccess() {
            return pathStyleAccess;
        }
    }

    public S3Directory(LockFactory lockFactory, S3Config config, String bucket, String pathPrefix) throws IOException {
        super(lockFactory);
        this.bucket = bucket;
        String path = pathPrefix == null ? "" : pathPrefix;
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        this.pathPrefix = path;
        this.s3Client = createS3Client(config);
    }

    /**
     * Create S3Client from configuration parameters
     */
    private static S3Client createS3Client(S3Config config) throws IOException {
        if (config.getAccessKey() == null || config.getSecretKey() == null) {
            throw new IOException("S3 access key and secret key are required");
        }

        try {
            AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
                    config.getAccessKey(),
                    config.getSecretKey()
            );

            S3ClientBuilder clientBuilder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                    .region(config.getRegion());

            // Configure endpoint if provided (for custom S3-compatible services)
            if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
                clientBuilder.endpointOverride(URI.create(config.getEndpoint()));
            }

            // Configure path style access if needed (required for some S3-compatible services)
            if (config.isPathStyleAccess()) {
                clientBuilder.serviceConfiguration(
                        b -> b.pathStyleAccessEnabled(true)
                );
            }

            return clientBuilder.build();
        } catch (Exception e) {
            throw new IOException("Failed to create S3Client: " + e.getMessage(), e);
        }
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(pathPrefix)
                    .build();

            List<String> files = new ArrayList<>();
            ListObjectsV2Response response;

            do {
                response = s3Client.listObjectsV2(request);
                for (S3Object s3Object : response.contents()) {
                    String key = s3Object.key();
                    if (key.startsWith(pathPrefix)) {
                        String fileName = key.substring(pathPrefix.length());
                        if (!fileName.isEmpty() && !pendingDeletes.contains(fileName)) {
                            files.add(fileName);
                        }
                    }
                }
                request = request.toBuilder()
                        .continuationToken(response.nextContinuationToken())
                        .build();
            } while (response.isTruncated());

            return files.toArray(new String[0]);
        } catch (Exception e) {
            throw new IOException("Failed to list objects in S3 bucket: " + bucket, e);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        String path = resolvePath(name);
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            s3Client.deleteObject(request);
            pendingDeletes.remove(name);
        } catch (Exception e) {
            // Add to pending deletes and try again later
            pendingDeletes.add(name);
            throw new IOException("Failed to delete file: " + name, e);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        String path = resolvePath(name);
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            HeadObjectResponse response = s3Client.headObject(request);
            return response.contentLength();
        } catch (NoSuchKeyException e) {
            throw new NoSuchFileException("File not found: " + name);
        } catch (Exception e) {
            throw new IOException("Failed to get file length: " + name, e);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        String path = resolvePath(name);
        pendingDeletes.remove(name); // Remove from pending deletes if it was there
        return new S3IndexOutput(name, path, bucket, s3Client);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String tempName = getTempFileName(prefix, suffix, System.nanoTime());
        return createOutput(tempName, context);
    }

    protected static String getTempFileName(String prefix, String suffix, long counter) {
        return prefix + "_" + Long.toString(counter, Character.MAX_RADIX) + "_" + suffix;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        // S3 writes are atomic, so sync is essentially a no-op
        // But we can verify files exist
        for (String name : names) {
            if (!fileExists(name)) {
                throw new IOException("File not found during sync: " + name);
            }
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        // S3 handles metadata automatically
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        String sourcePath = resolvePath(source);
        String destPath = resolvePath(dest);

        try {
            // Copy object to new location
            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .sourceBucket(bucket)
                    .sourceKey(sourcePath)
                    .destinationBucket(bucket)
                    .destinationKey(destPath)
                    .build();
            s3Client.copyObject(copyRequest);

            // Delete original
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(sourcePath)
                    .build();
            s3Client.deleteObject(deleteRequest);

            pendingDeletes.remove(source);
            pendingDeletes.remove(dest);
        } catch (Exception e) {
            throw new IOException("Failed to rename file from " + source + " to " + dest, e);
        }
    }

    private boolean fileExists(String name) {
        try {
            String path = resolvePath(name);
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    protected void ensureCanRead(String name) throws IOException {
        if (pendingDeletes.contains(name)) {
            throw new NoSuchFileException(
                    "file \"" + name + "\" is pending delete and cannot be opened for read");
        }
    }

    private String resolvePath(String name) {
        return pathPrefix == null ? name : pathPrefix + name;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        String path = resolvePath(name);
        boolean success = false;
        try {
            final S3BufferedInput indexInput = new S3BufferedInput(name, s3Client, bucket, path, context);
            success = true;
            return indexInput;
        } finally {
            if (!success) {
                // TODO handle failure
            }
        }
    }


    @Override
    public void close() throws IOException {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Set.copyOf(pendingDeletes);
    }

    /**
     * Returns the S3Client used by this directory.
     * Package-private for use by S3LockFactory.
     */
    S3Client getS3Client() {
        return s3Client;
    }

    /**
     * Returns the S3 bucket name used by this directory.
     * Package-private for use by S3LockFactory.
     */
    String getBucket() {
        return bucket;
    }

    /**
     * Returns the path prefix used by this directory, or empty string if none.
     * Package-private for use by S3LockFactory.
     */
    String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Factory method to create S3Directory with configuration
     */
    public static S3Directory open(S3Config config, String bucketName) throws IOException {
        return new S3Directory(S3LockFactory.getDefault(), config, bucketName, null);
    }

    /**
     * Factory method to create S3Directory with configuration and path prefix
     */
    public static S3Directory open(S3Config config, String bucketName, String pathPrefix) throws IOException {
        return new S3Directory(S3LockFactory.getDefault(), config, bucketName, pathPrefix);
    }

    /**
     * Factory method to create S3Directory with custom lock factory
     */
    public static S3Directory open(LockFactory lockFactory, S3Config config, String bucketName, String pathPrefix) throws IOException {
        return new S3Directory(lockFactory, config, bucketName, pathPrefix);
    }

    /**
     * Convenience factory method for common S3-compatible services
     */
    public static S3Directory openForMinio(String endpoint, String accessKey, String secretKey, String bucketName) throws IOException {
        S3Config config = new S3Config()
                .endpoint(endpoint)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .pathStyleAccess(true)
                .region("us-east-1");
        return open(config, bucketName);
    }

    /**
     * Convenience factory method for AWS S3
     */
    public static S3Directory openForAWS(String accessKey, String secretKey, String region, String bucketName) throws IOException {
        S3Config config = new S3Config()
                .accessKey(accessKey)
                .secretKey(secretKey)
                .region(region);
        return open(config, bucketName);
    }

    /**
     * @deprecated Use {@link #open(S3Config, String)} instead
     */
    @Deprecated
    public static S3Directory open(S3Client client, String bucketName) throws IOException {
        throw new UnsupportedOperationException("This method is deprecated. Use S3Directory.open(S3Config, String) instead.");
    }

    public static class S3BufferedInput extends BufferedIndexInput {

        private final S3Client client;
        private final String bucket;
        private final String path;
        private final long fileLength;

        public S3BufferedInput(String resourceDesc, S3Client s3Client, String bucket, String path, IOContext context) throws IOException {
            super(resourceDesc, context);
            this.client = s3Client;
            this.bucket = bucket;
            this.path = path;

            // Get file length from S3
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .build();
                HeadObjectResponse response = s3Client.headObject(request);
                this.fileLength = response.contentLength();
            } catch (NoSuchKeyException e) {
                throw new NoSuchFileException("File not found: " + path);
            } catch (Exception e) {
                throw new IOException("Failed to get file info: " + path, e);
            }
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            long pos = getFilePointer();
            int length = b.remaining();

            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .range("bytes=" + pos + "-" + (pos + length - 1))
                        .build();

                ResponseBytes<GetObjectResponse> response = client.getObjectAsBytes(request);
                byte[] data = response.asByteArray();

                if (data.length < length) {
                    throw new IOException("Unexpected EOF while reading from S3: expected " + length + " bytes, got " + data.length);
                }

                b.put(data, 0, Math.min(length, data.length));
            } catch (Exception e) {
                throw new IOException("Failed to read from S3: " + path + " at position " + pos, e);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            // BufferedIndexInput handles position tracking, no additional work needed
        }

        @Override
        public void close() throws IOException {
            // Nothing to close for S3 reads
        }

        @Override
        public long length() {
            return fileLength;
        }
    }

    public static class S3IndexOutput extends OutputStreamIndexOutput {

        /**
         * Custom OutputStream that buffers writes and uploads to S3 on close
         */
        private static class S3OutputStream extends OutputStream {
            private final String path;
            private final String bucket;
            private final S3Client client;
            private final ByteArrayOutputStream buffer;
            private boolean closed = false;

            public S3OutputStream(String path, String bucket, S3Client client) {
                this.path = path;
                this.bucket = bucket;
                this.client = client;
                this.buffer = new ByteArrayOutputStream();
            }

            @Override
            public void write(int b) throws IOException {
                ensureOpen();
                buffer.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                ensureOpen();
                buffer.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                ensureOpen();
                // For S3, we don't need to flush until close since we upload everything at once
            }

            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }

                try {
                    // Upload the entire buffer to S3
                    byte[] data = buffer.toByteArray();
                    PutObjectRequest request = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(path)
                            .contentLength((long) data.length)
                            .build();

                    client.putObject(request, RequestBody.fromBytes(data));
                } catch (Exception e) {
                    throw new IOException("Failed to write to S3: " + path, e);
                } finally {
                    closed = true;
                    buffer.close();
                }
            }

            private void ensureOpen() throws IOException {
                if (closed) {
                    throw new IOException("OutputStream is closed");
                }
            }
        }

        /** Default buffer size for S3 operations */
        private static final int DEFAULT_BUFFER_SIZE = 8192;

        public S3IndexOutput(String resourceDescription, String path, String bucket, S3Client client) {
            this(resourceDescription, path, bucket, client, DEFAULT_BUFFER_SIZE);
        }

        public S3IndexOutput(String resourceDescription, String path, String bucket, S3Client client, int bufferSize) {
            super(resourceDescription, path, new S3OutputStream(path, bucket, client), bufferSize);
        }
    }
}
