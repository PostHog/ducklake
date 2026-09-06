package com.posthog.hoglake.observability

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Emission isolation for the audit layer (adversarial-review items): a
 * committed action is never failed by its own audit decoration, a
 * broken audit appender never propagates out of [Audit.event] (it lands
 * on the application logger instead), and a failure-path decoration
 * error rides the original exception as a suppressed throwable.
 */
class AuditEmissionSafetyTest {
    private class ThrowingAppender : AppenderBase<ILoggingEvent>() {
        // doAppend, not append: AppenderBase.doAppend guards append() with
        // its own try/catch, so only a doAppend override actually makes
        // the logging call throw at the emission site.
        override fun doAppend(eventObject: ILoggingEvent) = throw IllegalStateException("appender is broken")

        override fun append(event: ILoggingEvent) = Unit
    }

    private class CollectingAppender : AppenderBase<ILoggingEvent>() {
        val events = CopyOnWriteArrayList<ILoggingEvent>()

        override fun append(event: ILoggingEvent) {
            events += event
        }
    }

    private val attached = mutableListOf<Pair<Logger, AppenderBase<ILoggingEvent>>>()

    private fun attach(
        loggerName: String,
        appender: AppenderBase<ILoggingEvent>,
    ) {
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        appender.context = ctx
        appender.start()
        val logger = LoggerFactory.getLogger(loggerName) as Logger
        logger.addAppender(appender)
        attached += logger to appender
    }

    @AfterEach
    fun detach() {
        for ((logger, appender) in attached) {
            logger.detachAppender(appender)
            appender.stop()
        }
        attached.clear()
    }

    @Test
    fun `event never propagates a broken appender - the app logger gets the error`() {
        attach(Audit.LOGGER_NAME, ThrowingAppender())
        val appLog = CollectingAppender()
        attach(Audit::class.java.name, appLog)

        // Must not throw, whatever the audit stream's appender does.
        Audit.event("test_action", "cat", "obj", "ok")

        assertThat(appLog.events)
            .describedAs("emission failure surfaces on the application logger")
            .anySatisfy { assertThat(it.formattedMessage).contains("test_action") }
    }

    @Test
    fun `audited returns the block's result even when success decoration throws`() {
        val result =
            Audit.audited(
                "test_action",
                "cat",
                "obj",
                detail = { throw IllegalStateException("detail lambda is broken") },
            ) { 42 }
        assertThat(result).isEqualTo(42)
    }

    @Test
    fun `audited returns the block's result even when successOutcome throws`() {
        val result =
            Audit.audited<String>(
                "test_action",
                "cat",
                "obj",
                successOutcome = { throw IllegalStateException("outcome lambda is broken") },
            ) { "done" }
        assertThat(result).isEqualTo("done")
    }

    @Test
    fun `a failure-path decoration error is suppressed onto the original exception`() {
        // An exception whose message getter itself throws: the failure
        // path's audit decoration blows up, and the ORIGINAL failure must
        // still be what propagates, with the decoration error suppressed.
        class HostileException : RuntimeException() {
            override val message: String
                get() = throw IllegalStateException("message getter is broken")
        }
        assertThatThrownBy {
            Audit.audited<Unit>("test_action", "cat", "obj") { throw HostileException() }
        }.isInstanceOf(HostileException::class.java)
            .satisfies({ e ->
                assertThat(e.suppressed)
                    .describedAs("decoration failure rides along as suppressed")
                    .isNotEmpty()
            })
    }
}
