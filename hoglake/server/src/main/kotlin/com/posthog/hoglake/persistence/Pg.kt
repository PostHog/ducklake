package com.posthog.hoglake.persistence

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.sql.SQLException

/**
 * Small Postgres-facing helpers shared by the repositories: SQLSTATE
 * classification for constraint violations and jsonb (de)serialization
 * for `hog_column.type_params`.
 */
internal object Pg {
    const val UNIQUE_VIOLATION = "23505"
    const val CHECK_VIOLATION = "23514"
    const val LOCK_NOT_AVAILABLE = "55P03"

    private val json: ObjectMapper = jacksonObjectMapper()
    private val mapType = object : TypeReference<Map<String, Any?>>() {}

    /** SQLSTATE of the underlying SQLException, or null if not one. */
    fun sqlState(e: UnableToExecuteStatementException): String? = (e.cause as? SQLException)?.sqlState

    fun isUniqueViolation(e: UnableToExecuteStatementException): Boolean = sqlState(e) == UNIQUE_VIOLATION

    fun isCheckViolation(e: UnableToExecuteStatementException): Boolean = sqlState(e) == CHECK_VIOLATION

    /** lock_timeout expiry ("canceling statement due to lock timeout"). */
    fun isLockTimeout(e: UnableToExecuteStatementException): Boolean = sqlState(e) == LOCK_NOT_AVAILABLE

    /** Serialize column type params for a jsonb column; null stays null. */
    fun toJson(params: Map<String, Any?>?): String? = params?.let { json.writeValueAsString(it) }

    /** Parse a jsonb column back into type params; null stays null. */
    fun fromJson(s: String?): Map<String, Any?>? = s?.let { json.readValue(it, mapType) }
}
