package org.apache.lucene.store;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * S3-based locking implementation. This class explicitly checks that
 * the passed {@link Directory} is an {@link S3Directory}.
 *
 * <p>This implementation uses S3 objects as distributed locks. When a lock is obtained,
 * a small object is created in S3. The lock is released by deleting the object.
 * This provides distributed locking across multiple instances accessing the same S3 bucket.
 *
 * <p>Lock validation is performed by checking if the S3 object still exists.
 * If the object is deleted externally, the lock becomes invalid.
 */
public class S3LockFactory extends LockFactory {

    private static final String LOCK_FILE_PREFIX = ".lock-";
    private static final String LOCK_CONTENT = "lucene-lock";

    // Keep track of obtained locks to prevent multiple locks with same name in same JVM
    private final ConcurrentMap<String, S3Lock> obtainedLocks = new ConcurrentHashMap<>();

    /** Returns a default S3LockFactory instance. */
    public static S3LockFactory getDefault() {
        return new S3LockFactory();
    }

    @Override
    public final Lock obtainLock(Directory dir, String lockName) throws IOException {
        if (!(dir instanceof S3Directory)) {
            throw new UnsupportedOperationException(
                getClass().getSimpleName()
                    + " can only be used with S3Directory subclasses, got: "
                    + dir);
        }
        return obtainS3Lock((S3Directory) dir, lockName);
    }

    /**
     * Obtain a lock for an S3Directory instance.
     *
     * @param dir the S3Directory to obtain lock for
     * @param lockName the name of the lock
     * @return the obtained lock
     * @throws IOException if the lock could not be obtained
     */
    protected Lock obtainS3Lock(S3Directory dir, String lockName) throws IOException {
        String lockKey = LOCK_FILE_PREFIX + lockName;

        // Check if we already have this lock in the current JVM
        S3Lock existingLock = obtainedLocks.get(lockKey);
        if (existingLock != null) {
            throw new LockObtainFailedException(
                "Lock already obtained in this JVM: " + lockName);
        }

        S3Lock lock = new S3Lock(dir, lockKey);

        // Try to create the lock object in S3
        try {
            lock.create();

            // Store the lock to prevent multiple acquisitions in same JVM
            S3Lock previousLock = obtainedLocks.putIfAbsent(lockKey, lock);
            if (previousLock != null) {
                // Another thread got the lock first, clean up and fail
                try {
                    lock.release();
                } catch (Exception e) {
                    // Ignore cleanup exceptions
                }
                throw new LockObtainFailedException(
                    "Lock already obtained in this JVM: " + lockName);
            }

            return lock;
        } catch (S3Exception e) {
            if (e.statusCode() == 412) { // Precondition failed - object already exists
                throw new LockObtainFailedException(
                    "Lock already held by another process: " + lockName);
            }
            throw new IOException("Failed to obtain S3 lock: " + lockName, e);
        }
    }

    /**
     * S3-based lock implementation
     */
    private class S3Lock extends Lock {
        private final S3Directory directory;
        private final String lockKey;
        private volatile boolean closed = false;

        S3Lock(S3Directory directory, String lockKey) {
            this.directory = directory;
            this.lockKey = lockKey;
        }

        /**
         * Constructs the full S3 key for the lock file
         */
        private String getFullKey() {
            String pathPrefix = directory.getPathPrefix();
            if (pathPrefix.isEmpty()) {
                return lockKey;
            }
            return pathPrefix + lockKey;
        }

        /**
         * Create the lock object in S3
         */
        void create() throws S3Exception {
            S3Client s3Client = directory.getS3Client();
            String bucket = directory.getBucket();
            String fullKey = getFullKey();

            // Use conditional put to ensure atomicity
            // The lock is obtained only if the object doesn't already exist
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(fullKey)
                .ifNoneMatch("*") // Only create if object doesn't exist
                .contentType("text/plain")
                .build();

            s3Client.putObject(request, RequestBody.fromString(LOCK_CONTENT));
        }

        /**
         * Release the lock by deleting the S3 object
         */
        void release() throws IOException {
            S3Client s3Client = directory.getS3Client();
            String bucket = directory.getBucket();
            String fullKey = getFullKey();

            try {
                DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(fullKey)
                    .build();

                s3Client.deleteObject(request);
            } catch (S3Exception e) {
                if (e.statusCode() != 404) { // Ignore if object already deleted
                    throw new IOException("Failed to release S3 lock: " + lockKey, e);
                }
            }
        }

        @Override
        public void ensureValid() throws IOException {
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }

            // Check if the lock object still exists in S3
            S3Client s3Client = directory.getS3Client();
            String bucket = directory.getBucket();
            String fullKey = getFullKey();

            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(fullKey)
                    .build();

                s3Client.headObject(request);
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
                    throw new AlreadyClosedException("Lock was invalidated (S3 object deleted): " + this);
                }
                throw new IOException("Failed to validate S3 lock: " + lockKey, e);
            }

            // Also check our local tracking
            if (!obtainedLocks.containsKey(lockKey)) {
                throw new AlreadyClosedException("Lock instance was invalidated from local map: " + this);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }

            try {
                // Remove from local tracking first
                obtainedLocks.remove(lockKey);

                // Then release the S3 object
                release();
            } finally {
                closed = true;
            }
        }

        @Override
        public String toString() {
            return "S3Lock@" + directory.getBucket() + ":" + lockKey;
        }
    }
}
