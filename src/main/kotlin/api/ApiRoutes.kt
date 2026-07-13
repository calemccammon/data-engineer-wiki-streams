package api

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import models.BotRatioState
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import streams.WikiTopology
import java.time.Instant

private val logger = KotlinLogging.logger {}
private val json = Json { ignoreUnknownKeys = true }

// ── Response DTOs ─────────────────────────────────────────────────────────────

@Serializable
data class EditCountResponse(
    val wiki: String,
    val windowStartIso: String,
    val windowEndIso: String,
    val editCount: Long,
)

@Serializable
data class HotPageResponse(
    val wiki: String,
    val title: String,
    val editCount: Long,
    val windowStartIso: String,
    val windowEndIso: String,
)

@Serializable
data class BotRatioResponse(
    val wiki: String,
    val totalEdits: Long,
    val botEdits: Long,
    val humanEdits: Long,
    val botRatioPct: Double,
)

@Serializable
data class HealthResponse(val status: String, val streamsState: String)

// ── Route definitions ─────────────────────────────────────────────────────────

/**
 * Registers all REST API routes.
 *
 * Interactive queries let us read directly from Kafka Streams' in-process
 * RocksDB state stores — no round-trip to Kafka, no external database needed.
 *
 * GET /api/health       — liveness check
 * GET /api/stats        — edit counts per wiki in the current 1-min tumbling window
 * GET /api/stats/{wiki} — edit counts for a specific wiki
 * GET /api/hot-pages    — most-edited article pages in the last 5 minutes
 * GET /api/bot-ratio    — running bot vs human edit ratio per wiki
 */
fun Application.configureRoutes(streams: KafkaStreams) {
    install(StatusPages) {
        exception<InvalidStateStoreException> { call, ex ->
            logger.warn { "State store not ready: ${ex.message}" }
            call.respond(
                HttpStatusCode.ServiceUnavailable,
                mapOf("error" to "State stores not ready yet — pipeline may still be starting up"),
            )
        }
        exception<Throwable> { call, ex ->
            logger.error(ex) { "Unhandled error" }
            call.respond(HttpStatusCode.InternalServerError, mapOf("error" to ex.message))
        }
    }

    routing {
        route("/api") {

            // ── Health ────────────────────────────────────────────────────────
            get("/health") {
                val state = streams.state().name
                val status = if (streams.state().isRunningOrRebalancing) "ok" else "degraded"
                call.respond(HealthResponse(status = status, streamsState = state))
            }

            // ── Edit counts (tumbling 1-min window) ───────────────────────────
            //
            // Queries the windowed state store for all keys in a time range.
            // Returns counts for the most recent completed 1-minute window.
            get("/stats") {
                val store: ReadOnlyWindowStore<String, Long> = streams.store(
                    org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                        WikiTopology.STORE_EDIT_COUNTS,
                        QueryableStoreTypes.windowStore(),
                    ),
                )
                val now = Instant.now()
                val windowStart = now.minusSeconds(120)

                val results = mutableListOf<EditCountResponse>()
                store.fetchAll(windowStart, now).use { iter ->
                    for (kv in iter) {
                        val wiki = kv.key.key()
                        val window = kv.key.window()
                        results += EditCountResponse(
                            wiki = wiki,
                            windowStartIso = window.startTime().toString(),
                            windowEndIso = window.endTime().toString(),
                            editCount = kv.value,
                        )
                    }
                }
                call.respond(results.sortedByDescending { it.editCount })
            }

            get("/stats/{wiki}") {
                val wiki = call.parameters["wiki"] ?: return@get call.respond(
                    HttpStatusCode.BadRequest, mapOf("error" to "wiki parameter required"),
                )
                val store: ReadOnlyWindowStore<String, Long> = streams.store(
                    org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                        WikiTopology.STORE_EDIT_COUNTS,
                        QueryableStoreTypes.windowStore(),
                    ),
                )
                val now = Instant.now()
                val results = mutableListOf<EditCountResponse>()
                store.fetch(wiki, now.minusSeconds(300), now).use { iter ->
                    for (kv in iter) {
                        // WindowStoreIterator key = window start ms (Long), value = count
                        results += EditCountResponse(
                            wiki = wiki,
                            windowStartIso = Instant.ofEpochMilli(kv.key).toString(),
                            windowEndIso = Instant.ofEpochMilli(kv.key + 60_000L).toString(),
                            editCount = kv.value,
                        )
                    }
                }
                call.respond(results)
            }

            // ── Hot pages (hopping 5-min window) ──────────────────────────────
            //
            // Returns the top 20 most-edited article pages in the last 5 minutes.
            // Key format in the store: "wiki|page title"
            get("/hot-pages") {
                val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: 20
                val store: ReadOnlyWindowStore<String, Long> = streams.store(
                    org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                        WikiTopology.STORE_HOT_PAGES,
                        QueryableStoreTypes.windowStore(),
                    ),
                )
                val now = Instant.now()
                val windowStart = now.minusSeconds(300)

                val results = mutableListOf<HotPageResponse>()
                store.fetchAll(windowStart, now).use { iter ->
                    for (kv in iter) {
                        val (wiki, title) = kv.key.key().split("|", limit = 2)
                        val window = kv.key.window()
                        results += HotPageResponse(
                            wiki = wiki,
                            title = title,
                            editCount = kv.value,
                            windowStartIso = window.startTime().toString(),
                            windowEndIso = window.endTime().toString(),
                        )
                    }
                }
                call.respond(
                    results
                        .groupBy { it.wiki to it.title }
                        .mapValues { (_, v) -> v.maxByOrNull { it.editCount }!! }
                        .values
                        .sortedByDescending { it.editCount }
                        .take(limit),
                )
            }

            // ── Bot ratio (running KTable) ────────────────────────────────────
            //
            // Returns cumulative bot vs human edit stats for every wiki seen so far.
            // This is a plain KTable query (not windowed) — always reflects latest state.
            get("/bot-ratio") {
                val store: ReadOnlyKeyValueStore<String, BotRatioState> = streams.store(
                    org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                        WikiTopology.STORE_BOT_RATIO,
                        QueryableStoreTypes.keyValueStore(),
                    ),
                )
                val results = mutableListOf<BotRatioResponse>()
                store.all().use { iter ->
                    for (kv in iter) {
                        val state = kv.value
                        results += BotRatioResponse(
                            wiki = kv.key,
                            totalEdits = state.totalEdits,
                            botEdits = state.botEdits,
                            humanEdits = state.humanEdits,
                            botRatioPct = (state.botRatio * 100).let { "%.2f".format(it).toDouble() },
                        )
                    }
                }
                call.respond(results.sortedByDescending { it.totalEdits })
            }
        }
    }
}
