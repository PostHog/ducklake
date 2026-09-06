package com.posthog.hoglake

import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The one home for every periodic background loop (hydrator sweep,
 * expiry, cleanup drain, compaction, metrics sampler): coroutines under
 * a single [SupervisorJob]-rooted scope owned by the App lifecycle,
 * replacing the per-service daemon threads.
 *
 * Contracts:
 *  - `intervalMs <= 0` = loop disabled ([register] is a no-op), exactly
 *    as the old per-service startLoop behaved; tests keep driving the
 *    services' runOnce/sampleOnce entry points directly.
 *  - Per-loop isolation: one iteration's exception is caught INSIDE the
 *    loop — logged, counted (`hoglake_background_loop_failures_total{loop}`),
 *    and the loop sleeps its interval and runs again. The supervisor
 *    root is belt-and-braces: even an exception that somehow escaped a
 *    child could not cancel the siblings.
 *  - Loop bodies are blocking (JDBC/S3), so they run on [Dispatchers.IO]
 *    inside [runInterruptible]: cancellation interrupts a mid-body
 *    blocking call instead of waiting it out.
 *  - [close] is structured shutdown: cancel the supervisor, then join
 *    every loop with a hard [SHUTDOWN_TIMEOUT_MS] bound — shutdown may
 *    be slow to let an iteration finish, but can never hang.
 */
class BackgroundLoops : AutoCloseable {
    private val log = KotlinLogging.logger {}
    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor + Dispatchers.IO)
    private val loops = mutableListOf<Job>()

    /**
     * Register a periodic loop: run [body], sleep [intervalMs], repeat.
     * [intervalMs] <= 0 registers nothing (the disabled contract).
     */
    fun register(
        name: String,
        intervalMs: Long,
        body: () -> Unit,
    ) {
        if (intervalMs <= 0) return
        loops +=
            scope.launch(CoroutineName(name)) {
                while (true) {
                    try {
                        runInterruptible { body() }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        // One bad iteration never stops the loop (or any
                        // other loop): log, count, run again next interval.
                        log.error(e) { "background loop '$name' iteration failed; continuing" }
                        Metrics.backgroundLoopFailure(name)
                    }
                    delay(intervalMs)
                }
            }
    }

    /** Cancel every loop and wait (bounded) for them to finish. Idempotent. */
    override fun close() {
        supervisor.cancel()
        val clean =
            runBlocking {
                withTimeoutOrNull(SHUTDOWN_TIMEOUT_MS) { loops.joinAll() } != null
            }
        if (!clean) {
            log.warn { "background loops did not stop within ${SHUTDOWN_TIMEOUT_MS}ms; abandoning" }
        }
    }

    companion object {
        /** Hard bound on shutdown: never hang the process on a stuck loop. */
        const val SHUTDOWN_TIMEOUT_MS = 5_000L
    }
}
