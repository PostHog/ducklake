package com.posthog.hoglake.trino;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HoglakeConfigTest
{
    @Test
    void parsesFullConfig()
    {
        HoglakeConfig config = HoglakeConfig.fromMap(Map.of(
                "hoglake.uri", "http://hoglake:8080/",
                "hoglake.catalog", "lake",
                "hoglake.client.request-timeout", "45s",
                "hoglake.s3.endpoint", "http://minio:9000",
                "hoglake.s3.region", "eu-west-1",
                "hoglake.s3.access-key", "ak",
                "hoglake.s3.secret-key", "sk",
                "hoglake.s3.path-style", "true"));
        assertThat(config.uri()).isEqualTo("http://hoglake:8080"); // trailing slash trimmed
        assertThat(config.catalog()).isEqualTo("lake");
        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.s3Endpoint()).isEqualTo("http://minio:9000");
        assertThat(config.s3Region()).isEqualTo("eu-west-1");
        assertThat(config.s3PathStyle()).isTrue();
    }

    @Test
    void appliesDefaults()
    {
        HoglakeConfig config = HoglakeConfig.fromMap(Map.of("hoglake.uri", "http://h:1"));
        assertThat(config.catalog()).isEqualTo("hoglake");
        assertThat(config.requestTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.s3Endpoint()).isNull();
        assertThat(config.s3Region()).isEqualTo("us-east-1");
        assertThat(config.s3PathStyle()).isFalse();
    }

    @Test
    void requiresUri()
    {
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of("hoglake.catalog", "lake")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.uri");
    }

    @Test
    void allTrailingSlashesAreCollapsed()
    {
        // Regression (S7): single-trim used to leave "http://h:8080//" with
        // one trailing slash, producing "...//v1/..." request paths.
        assertThat(HoglakeConfig.fromMap(Map.of("hoglake.uri", "http://h:8080//")).uri())
                .isEqualTo("http://h:8080");
        assertThat(HoglakeConfig.fromMap(Map.of("hoglake.uri", "http://h:8080///")).uri())
                .isEqualTo("http://h:8080");
    }

    @Test
    void emptyCatalogValueIsRejectedAtLoadTime()
    {
        // Regression (S7): `hoglake.catalog=` (present but empty) used to be
        // kept as "", producing /v1/catalogs//namespaces requests. An
        // explicit empty value is a typo — reject at load, not per query.
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.catalog", "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.catalog");
    }

    @Test
    void malformedUriIsRejectedAtLoadTime()
    {
        // Regression (S7): a bad port or missing scheme used to be accepted
        // here and fail every query at request-build time instead.
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of("hoglake.uri", "http://h:notaport")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.uri");
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of("hoglake.uri", "not a uri at all")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.uri");
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of("hoglake.uri", "ftp://h:1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme");
    }

    @Test
    void requestTimeoutParsesAirliftStyleDurations()
    {
        assertThat(timeout("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(timeout("30s")).isEqualTo(Duration.ofSeconds(30));
        assertThat(timeout("2m")).isEqualTo(Duration.ofMinutes(2));
        assertThat(timeout("1h")).isEqualTo(Duration.ofHours(1));
        assertThat(timeout("1.5s")).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void malformedRequestTimeoutIsRejectedAtLoadTime()
    {
        assertThatThrownBy(() -> timeout("30"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.client.request-timeout");
        assertThatThrownBy(() -> timeout("fast"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.client.request-timeout");
        assertThatThrownBy(() -> timeout("0s"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void missingS3CredentialsFallBackToDefaultProviderChain()
    {
        // Documented behavior: absent/empty s3 keys become null, which
        // S3FileSystemConfig treats as "use the AWS default credential
        // chain" (instance role, env vars). Note: trino-filesystem-s3 never
        // sends unsigned/anonymous requests, so a public bucket read on a
        // credential-less host still fails — with an SdkClientException at
        // first split read, not a config-time error.
        HoglakeConfig config = HoglakeConfig.fromMap(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.s3.access-key", "",
                "hoglake.s3.secret-key", ""));
        assertThat(config.s3AccessKey()).isNull();
        assertThat(config.s3SecretKey()).isNull();
    }

    @Test
    void rejectsUnknownHoglakeKeys()
    {
        assertThatThrownBy(() -> HoglakeConfig.fromMap(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.s3.pathstyle", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.s3.pathstyle");
    }

    private static Duration timeout(String value)
    {
        return HoglakeConfig.fromMap(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.client.request-timeout", value)).requestTimeout();
    }
}
