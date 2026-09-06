plugins {
    kotlin("jvm") version "2.2.0"
    application
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2"
}

group = "com.posthog.hoglake"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val ktorVersion = "3.1.3"
val jdbiVersion = "3.45.4"
val flywayVersion = "10.21.0"
// >= 1.21.1: older versions pin Docker API 1.32, which OrbStack's Docker 29 rejects.
val testcontainersVersion = "1.21.3"
val awsSdkVersion = "2.29.29"

dependencies {
    // Background loops (BackgroundLoops.kt): explicit pin of the
    // kotlinx-coroutines line ktor already carries transitively.
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")

    // HTTP server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.2")

    // Persistence
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:6.2.1")
    implementation("org.jdbi:jdbi3-core:$jdbiVersion")
    implementation("org.jdbi:jdbi3-kotlin:$jdbiVersion")
    implementation("org.jdbi:jdbi3-postgres:$jdbiVersion")
    implementation("org.flywaydb:flyway-core:$flywayVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")

    // Object store
    implementation("software.amazon.awssdk:s3:$awsSdkVersion")

    // parquet-java is THE parquet library (decision 2026-09-05: Hardwood is
    // out entirely — field ids are a contract, and parquet-java reads AND
    // writes them): the hydrator's footer reads and the compaction rewrite
    // writer both live on it. hadoop-client-api is the shaded,
    // dependency-free jar; the wider Hadoop dependency tree must not leak
    // into the codebase.
    implementation("org.apache.parquet:parquet-hadoop:1.15.2")
    implementation("org.apache.hadoop:hadoop-client-api:3.4.1")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.1")

    // Deletion vectors: hoglake DVs are Iceberg v3 puffin `deletion-vector-v1`
    // blobs — a portable 64-bit roaring bitmap of deleted row positions.
    // Compaction applies DVs at rewrite time (PuffinDeletionVector.kt), and
    // the Java RoaringBitmap serialize/deserialize format IS the portable
    // interoperable format the spec requires.
    implementation("org.roaringbitmap:RoaringBitmap:1.3.0")

    // Logging + observability
    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.3")
    implementation("net.logstash.logback:logstash-logback-encoder:8.0")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:1.14.2")

    // Tests
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:minio:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testImplementation("io.kotest:kotest-property:5.9.1")

    // Coverage-guided fuzzing (fuzzing.md layer 4): jazzer-junit @FuzzTest
    // targets in src/test/kotlin/com/posthog/hoglake/fuzz. Inside the normal
    // :test run they replay the committed corpus deterministically
    // (regression mode); the `fuzz` task reruns the same targets under
    // libFuzzer with a time budget (see the fuzzing tasks below).
    testImplementation("com.code-intelligence:jazzer-junit:0.24.0")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.posthog.hoglake.MainKt")
}

tasks.test {
    useJUnitPlatform()
    // Integration tests need Docker (Testcontainers); tag-gated so `gradle
    // test -PunitOnly` stays runnable without it.
    if (project.hasProperty("unitOnly")) {
        systemProperty("junit.jupiter.tags.exclude", "integration")
        exclude("**/*IntegrationTest*")
    }
    // jazzer-junit self-attaches its instrumentation agent for the corpus
    // replay of the fuzz targets; JDK 21 warns on dynamic attach otherwise.
    jvmArgs("-XX:+EnableDynamicAgentLoading")
    testLogging {
        events("failed", "skipped")
        showStackTraces = true
    }
}

// ---- fuzzing (fuzzing.md layer 4) -----------------------------------------
//
// `./gradlew fuzz -PfuzzSeconds=300` runs every @FuzzTest target under
// libFuzzer for the given per-target budget (default 60s). jazzer-junit
// permits one fuzz test per JVM run, so each target gets its own Test task,
// chained sequentially. Committed seed corpus lives under
// src/test/resources/com/posthog/hoglake/fuzz/<Target>Inputs/<method>/
// (jazzer-junit's inputs convention — the same files the normal :test run
// replays deterministically); crashing inputs found while fuzzing are
// written back into those directories, and the growing generated corpus
// lands in .cifuzz-corpus/ (transient, not committed).

val fuzzTargets =
    listOf(
        "IcebergSingleValueDecodeFuzzTest",
        "IcebergSingleValueCompareFuzzTest",
        "ParquetFooterFuzzTest",
        "PuffinDeletionVectorFuzzTest",
        "IdentifiersFuzzTest",
        "WireDtoParseFuzzTest",
    )

val fuzzSeconds = (project.findProperty("fuzzSeconds") as String?)?.toLongOrNull() ?: 60L

val fuzzTasks =
    fuzzTargets.map { target ->
        tasks.register<Test>("fuzz$target") {
            description = "Coverage-guided Jazzer run of $target (budget ${fuzzSeconds}s)"
            group = "verification"
            testClassesDirs = sourceSets.test.get().output.classesDirs
            classpath = sourceSets.test.get().runtimeClasspath
            useJUnitPlatform()
            filter { includeTestsMatching("com.posthog.hoglake.fuzz.$target") }
            // Truthy JAZZER_FUZZ (env var or system property) flips
            // jazzer-junit from corpus replay to real fuzzing.
            systemProperty("JAZZER_FUZZ", "1")
            // Budget override: extra libFuzzer args are appended after the
            // annotation's -max_total_time, and the last occurrence wins.
            // (arg 0 is argv0 and skipped by jazzer-junit.)
            systemProperty("jazzer.internal.arg.0", "jazzer")
            systemProperty("jazzer.internal.arg.1", "-max_total_time=$fuzzSeconds")
            jvmArgs("-XX:+EnableDynamicAgentLoading")
            outputs.upToDateWhen { false }
            testLogging {
                events("passed", "failed")
                showStackTraces = true
                showStandardStreams = true
            }
        }
    }

// Serialize the per-target runs: concurrent libFuzzer instances would fight
// over CPU and the shared build directory.
fuzzTasks.zipWithNext().forEach { (a, b) -> b.configure { mustRunAfter(a) } }

tasks.register("fuzz") {
    description = "Run every Jazzer fuzz target for -PfuzzSeconds seconds each (default 60)"
    group = "verification"
    dependsOn(fuzzTasks)
}

// One-shot (manual) seed-corpus generator: writes the committed corpus under
// src/test/resources/com/posthog/hoglake/fuzz from the cross-language vector
// file plus freshly built parquet footers / puffin DV blobs. Rerun only when
// adding targets or new vector-derived seeds; the output is committed.
tasks.register<JavaExec>("generateFuzzSeeds") {
    description = "Regenerate the committed fuzz seed corpus (manual)"
    group = "verification"
    mainClass.set("com.posthog.hoglake.fuzz.FuzzSeedGenerator")
    classpath = sourceSets.test.get().runtimeClasspath
    args(
        layout.projectDirectory.dir("src/test/resources/com/posthog/hoglake/fuzz").asFile.absolutePath,
        layout.projectDirectory.file("../pyhoglake/tests/vectors/bounds_vectors.json").asFile.absolutePath,
    )
}
