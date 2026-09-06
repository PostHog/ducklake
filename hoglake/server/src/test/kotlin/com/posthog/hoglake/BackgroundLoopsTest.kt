package com.posthog.hoglake

import com.posthog.hoglake.observability.Metrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The coroutine loop runner's contracts: disabled-by-interval, prompt
 * bounded shutdown even mid-body, and per-iteration/per-loop failure
 * isolation (a crashing iteration is logged + counted and the NEXT
 * iteration still runs; sibling loops never notice).
 */
class BackgroundLoopsTest {
    @Test
    fun `non-positive interval registers nothing and close is a no-op`() {
        val ran = AtomicInteger(0)
        BackgroundLoops().use { loops ->
            loops.register("zero", 0) { ran.incrementAndGet() }
            loops.register("negative", -1) { ran.incrementAndGet() }
            Thread.sleep(100)
        }
        assertThat(ran.get()).isEqualTo(0)
    }

    @Test
    fun `close cancels promptly - a loop blocked mid-body is interrupted, not waited out`() {
        val entered = CountDownLatch(1)
        val loops = BackgroundLoops()
        // One loop parked deep inside a blocking body...
        loops.register("blocked", 60_000) {
            entered.countDown()
            try {
                Thread.sleep(60_000)
            } catch (e: InterruptedException) {
                // runInterruptible delivers cancellation as an interrupt.
                throw e
            }
        }
        // ...and one parked in its between-iterations delay.
        loops.register("sleeping", 60_000) { }
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

        val start = System.nanoTime()
        loops.close()
        val elapsedMs = (System.nanoTime() - start) / 1_000_000
        assertThat(elapsedMs)
            .describedAs("shutdown must interrupt blocked bodies, not wait them out")
            .isLessThan(BackgroundLoops.SHUTDOWN_TIMEOUT_MS)
    }

    @Test
    fun `a crashing iteration does not stop later iterations or sibling loops`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            val crashes = AtomicInteger(0)
            val healthy = AtomicInteger(0)
            BackgroundLoops().use { loops ->
                loops.register("crashy", 10) {
                    crashes.incrementAndGet()
                    throw IllegalStateException("boom")
                }
                loops.register("healthy", 10) { healthy.incrementAndGet() }
                await().atMost(Duration.ofSeconds(10)).untilAsserted {
                    // The crashy loop RAN AGAIN after failing, and the
                    // sibling loop never stopped.
                    assertThat(crashes.get()).isGreaterThanOrEqualTo(3)
                    assertThat(healthy.get()).isGreaterThanOrEqualTo(3)
                }
            }
            val counted =
                registry.get("hoglake_background_loop_failures_total")
                    .tag("loop", "crashy")
                    .counter()
                    .count()
            assertThat(counted).isGreaterThanOrEqualTo(3.0)
        } finally {
            Metrics.clear()
        }
    }
}
