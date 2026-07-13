package producer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.utils.io.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import models.WikiEdit
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

private val log = KotlinLogging.logger {}

/**
 * Reads the Wikimedia Server-Sent Events (SSE) stream of recent changes and
 * publishes each "edit" or "new" event to Kafka as a JSON string.
 *
 * Key concepts demonstrated:
 * - SSE: a unidirectional push protocol (unlike WebSocket, no client→server messages)
 * - The Wikimedia stream is public, requires no authentication, and emits ~1000 events/min
 * - Kafka key = wiki name (e.g. "enwiki") — ensures same-wiki edits go to the same partition
 *   and allows Kafka Streams to do keyed aggregations without repartitioning
 *
 * Usage:
 *   Launch as a coroutine alongside the Streams app, e.g.:
 *     val job = scope.launch { WikiProducer().run() }
 */
class WikiProducer(
    private val bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092",
    private val topic: String = System.getenv("KAFKA_TOPIC_RAW_EDITS") ?: "wiki.raw-edits",
    private val sseUrl: String = System.getenv("WIKIMEDIA_SSE_URL")
        ?: "https://stream.wikimedia.org/v2/stream/recentchange",
) {
    private val json = Json {
        ignoreUnknownKeys = true   // Wikimedia sends many fields we don't model
        coerceInputValues = true   // tolerate null → default for non-nullable fields
    }

    private fun buildKafkaProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            // Idempotent producer — prevents duplicate messages on retries
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, "3")
        }
        return KafkaProducer(props)
    }

    /**
     * Connect to the SSE stream and publish events indefinitely.
     * Suspend until the coroutine is cancelled (e.g. on application shutdown).
     */
    suspend fun run() {
        val producer = buildKafkaProducer()
        val httpClient = HttpClient(CIO) {
            install(HttpTimeout) {
                // No request timeout — SSE streams are long-lived by design
                requestTimeoutMillis = HttpTimeout.INFINITE_TIMEOUT_MS
                connectTimeoutMillis = 10_000
            }
        }

        var published = 0L
        var skipped = 0L

        log.info { "WikiProducer starting — connecting to $sseUrl" }

        try {
            httpClient.prepareGet(sseUrl).execute { response ->
                val channel: ByteReadChannel = response.bodyAsChannel()

                // SSE format: lines starting with "data: " carry the JSON payload.
                // Events are separated by blank lines.
                val buffer = StringBuilder()

                while (!channel.isClosedForRead) {
                    val line = channel.readUTF8Line() ?: break

                    when {
                        line.startsWith("data: ") -> buffer.append(line.removePrefix("data: "))
                        line.isEmpty() && buffer.isNotEmpty() -> {
                            val raw = buffer.toString()
                            buffer.clear()

                            val edit = tryParse(raw)
                            if (edit == null || edit.type !in setOf("edit", "new") || edit.namespace != 0) {
                                skipped++
                            } else {
                                // Key by wiki so Kafka Streams can aggregate per-wiki without shuffle
                                val record = ProducerRecord(topic, edit.wiki, raw)
                                producer.send(record) { meta, ex ->
                                    if (ex != null) {
                                        log.error(ex) { "Failed to send record" }
                                    } else if (++published % 500 == 0L) {
                                        log.info { "Published $published edits (skipped $skipped) — partition ${meta.partition()} offset ${meta.offset()}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (e: CancellationException) {
            log.info { "WikiProducer cancelled — shutting down" }
        } catch (e: Exception) {
            log.error(e) { "SSE stream error — will reconnect" }
            throw e
        } finally {
            producer.flush()
            producer.close()
            httpClient.close()
            log.info { "WikiProducer stopped. Published=$published skipped=$skipped" }
        }
    }

    private fun tryParse(raw: String): WikiEdit? = try {
        json.decodeFromString<WikiEdit>(raw)
    } catch (e: Exception) {
        log.debug { "Unparseable event: ${e.message}" }
        null
    }
}
