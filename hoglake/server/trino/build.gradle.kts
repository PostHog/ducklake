// The native Trino connector for hoglake (read-only v1).
//
// Trino/SPI version: 446 — the last Trino line whose artifacts ship
// Java 21 bytecode (447+ requires Java 22, and its trino-spi is class
// file 66). The flox env provides JDK 21, so 446 is the newest SPI this
// build can compile against; the runtime is always the trinodb/trino:446
// container, which bundles its own JDK.
//
// The connector itself is plain Java (Trino SPI is Java-first); the
// Kotlin plugin is applied only for the test helper that boots the
// hoglake server in-process (root-project classes are Kotlin).
plugins {
    java
    kotlin("jvm")
}

group = "com.posthog.hoglake"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val trinoVersion = "446"
val testcontainersVersion = "1.21.3"
val ktorVersion = "3.1.3"

dependencies {
    // Provided by the Trino server's plugin classloader — never bundled.
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // Data plane: Trino's own parquet reader + S3 filesystem, the same
    // libraries the bundled hive/iceberg/delta connectors use.
    implementation("io.trino:trino-parquet:$trinoVersion")
    implementation("io.trino:trino-filesystem:$trinoVersion")
    implementation("io.trino:trino-filesystem-s3:$trinoVersion")
    implementation("io.trino:trino-memory-context:$trinoVersion")

    // Control plane: plain java.net.http + Jackson against the hoglake REST API.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")

    // Unit tests.
    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")

    // Integration tests: the hoglake server runs in-process from the
    // root project's main classes (patterns copied from the root test
    // fixtures — cross-subproject test-source imports are not a thing).
    testImplementation(project(":"))
    // Compile-time visibility for the in-process server helper (these are
    // `implementation` deps of the root project, so not exported to us).
    testImplementation("io.ktor:ktor-server-core:$ktorVersion")
    testImplementation("io.ktor:ktor-server-netty:$ktorVersion")
    testImplementation("com.zaxxer:HikariCP:6.2.1")
    testImplementation("org.jdbi:jdbi3-core:3.45.4")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:minio:$testcontainersVersion")
    testImplementation("org.testcontainers:trino:$testcontainersVersion")
    testImplementation("io.trino:trino-jdbc:$trinoVersion")
    // Real parquet files for the end-to-end read test (same writer the
    // root hydrator tests use).
    testImplementation("dev.hardwood:hardwood-core:1.1.0.Beta1")
    // Bucket creation + parquet upload to MinIO.
    testImplementation("software.amazon.awssdk:s3:2.29.29")
}

kotlin {
    jvmToolchain(21)
}

/**
 * Assemble the Trino plugin directory layout: the connector jar plus its
 * full runtime dependency closure, suitable for mounting at
 * /usr/lib/trino/plugin/hoglake. trino-spi (and anything else the server
 * parent classloader provides is harmless to duplicate; the SPI itself is
 * excluded defensively even though its Maven scope already keeps it off
 * the runtime classpath).
 */
val trinoPlugin by tasks.registering(Sync::class) {
    into(layout.buildDirectory.dir("trino-plugin/hoglake"))
    from(tasks.jar)
    from(configurations.runtimeClasspath) {
        exclude("trino-spi-*.jar")
        // The Kotlin plugin (applied only for the test-source server
        // helper) injects the stdlib into runtimeClasspath; the connector
        // itself is pure Java and does not need it.
        exclude("kotlin-stdlib-*.jar")
        exclude("annotations-13.0.jar")
    }
}

tasks.build {
    dependsOn(trinoPlugin)
}

tasks.test {
    useJUnitPlatform()
    dependsOn(trinoPlugin)
    // Integration tests need Docker (Testcontainers); tag-gated so
    // `gradle test -PunitOnly` stays runnable without it.
    if (project.hasProperty("unitOnly")) {
        systemProperty("junit.jupiter.tags.exclude", "integration")
        exclude("**/*IntegrationTest*")
    }
    // The assembled plugin directory (mounted at /usr/lib/trino/plugin/hoglake).
    // Declared as an input so plugin-layout changes re-run the tests.
    inputs.dir(trinoPlugin.map { it.destinationDir })
    systemProperty(
        "hoglake.trino.plugin.dir",
        trinoPlugin.get().destinationDir.absolutePath,
    )
    testLogging {
        events("failed", "skipped")
        showStackTraces = true
    }
}
