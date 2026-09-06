package com.posthog.hoglake.hydrator

import com.posthog.hoglake.Config
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.net.URI

/**
 * Thin S3/MinIO wrapper for the hydrator. Speaks `s3://bucket/key` URIs
 * (the absolute object-store paths stored in `hog_data_file.path`).
 */
class ObjectStore(
    endpoint: String?,
    region: String,
    accessKey: String?,
    secretKey: String?,
    pathStyle: Boolean,
) : AutoCloseable {
    constructor(config: Config) : this(
        endpoint = config.s3Endpoint.ifBlank { null },
        region = config.s3Region,
        accessKey = config.s3AccessKey.ifBlank { null },
        secretKey = config.s3SecretKey.ifBlank { null },
        pathStyle = config.s3PathStyle,
    )

    private val s3: S3Client =
        S3Client.builder()
            .region(Region.of(region))
            .apply {
                if (endpoint != null) endpointOverride(URI.create(endpoint))
                if (accessKey != null && secretKey != null) {
                    credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
                    )
                }
            }
            .forcePathStyle(pathStyle)
            .build()

    data class Location(val bucket: String, val key: String)

    /** Fetch the whole object at [pathUri] (`s3://bucket/key`). */
    fun get(pathUri: String): ByteArray {
        val loc = parse(pathUri)
        return s3.getObjectAsBytes(
            GetObjectRequest.builder().bucket(loc.bucket).key(loc.key).build(),
        ).asByteArray()
    }

    /** Ranged read of the object's tail, from byte [startInclusive] to the end. */
    fun getTail(
        pathUri: String,
        startInclusive: Long,
    ): ByteArray {
        require(startInclusive >= 0) { "negative range start $startInclusive for $pathUri" }
        return getRange(pathUri, "bytes=$startInclusive-")
    }

    private fun getRange(
        pathUri: String,
        range: String,
    ): ByteArray {
        val loc = parse(pathUri)
        return s3.getObjectAsBytes(
            GetObjectRequest.builder()
                .bucket(loc.bucket)
                .key(loc.key)
                .range(range)
                .build(),
        ).asByteArray()
    }

    fun put(
        pathUri: String,
        bytes: ByteArray,
    ) {
        val loc = parse(pathUri)
        s3.putObject(
            PutObjectRequest.builder().bucket(loc.bucket).key(loc.key).build(),
            RequestBody.fromBytes(bytes),
        )
    }

    /** Idempotent bucket creation (test/bootstrap convenience). */
    fun createBucket(bucket: String) {
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
        } catch (_: BucketAlreadyOwnedByYouException) {
            // fine
        } catch (_: BucketAlreadyExistsException) {
            // fine
        }
    }

    override fun close() = s3.close()

    companion object {
        private const val SCHEME = "s3://"

        /** Parse `s3://bucket/key`; throws [IllegalArgumentException] on anything else. */
        fun parse(pathUri: String): Location {
            require(pathUri.startsWith(SCHEME)) { "not an s3:// URI: $pathUri" }
            val rest = pathUri.removePrefix(SCHEME)
            val slash = rest.indexOf('/')
            require(slash > 0) { "s3 URI has no key: $pathUri" }
            val bucket = rest.substring(0, slash)
            val key = rest.substring(slash + 1)
            require(key.isNotEmpty()) { "s3 URI has empty key: $pathUri" }
            return Location(bucket, key)
        }
    }
}
