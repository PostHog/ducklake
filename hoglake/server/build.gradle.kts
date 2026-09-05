plugins {
    kotlin("jvm") version "2.2.0"
    application
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

    // Object store + parquet footers
    implementation("software.amazon.awssdk:s3:$awsSdkVersion")
    implementation("dev.hardwood:hardwood-core:1.1.0.Beta1")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.3")

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
    testLogging {
        events("failed", "skipped")
        showStackTraces = true
    }
}
