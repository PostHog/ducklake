package com.posthog.hoglake.hydrator

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ObjectStorePathTest {
    @Test
    fun `parses bucket and key`() {
        assertThat(ObjectStore.parse("s3://bucket/key"))
            .isEqualTo(ObjectStore.Location("bucket", "key"))
    }

    @Test
    fun `key keeps its slashes`() {
        assertThat(ObjectStore.parse("s3://my-bucket/a/b/c.parquet"))
            .isEqualTo(ObjectStore.Location("my-bucket", "a/b/c.parquet"))
    }

    @Test
    fun `key may contain odd characters`() {
        assertThat(ObjectStore.parse("s3://b/table=1/part 0001.parquet"))
            .isEqualTo(ObjectStore.Location("b", "table=1/part 0001.parquet"))
    }

    @Test
    fun `rejects non-s3 schemes`() {
        assertThatThrownBy { ObjectStore.parse("gs://bucket/key") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectStore.parse("/local/path.parquet") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectStore.parse("s3:/bucket/key") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects missing bucket or key`() {
        assertThatThrownBy { ObjectStore.parse("s3://") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectStore.parse("s3://bucket") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectStore.parse("s3://bucket/") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectStore.parse("s3:///key") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
