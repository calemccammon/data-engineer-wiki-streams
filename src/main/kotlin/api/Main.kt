package api

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import producer.WikiProducer
import streams.StreamsApp

private val log = KotlinLogging.logger {}

/**
 * Application entry point.
 *
 * Starts three components concurrently:
 *  1. WikiProducer — SSE → Kafka (runs as a coroutine)
 *  2. StreamsApp   — Kafka Streams topology (runs on its own thread pool)
 *  3. Ktor server  — REST API (serves interactive queries on state stores)
 *
 * Shutdown is coordinated: SIGINT/SIGTERM stops all three cleanly.
 */
fun main() {
    val streamsApp = StreamsApp()
    val kafkaStreams = streamsApp.start()

    // Give Streams a moment to restore state from Kafka before accepting queries
    log.info { "Waiting for Kafka Streams to reach RUNNING state..." }
    var waited = 0
    while (!kafkaStreams.state().isRunningOrRebalancing && waited < 30) {
        Thread.sleep(1000)
        waited++
    }
    log.info { "Kafka Streams state: ${kafkaStreams.state()}" }

    // Launch the Wikimedia SSE producer as a background coroutine
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val producerJob = scope.launch {
        while (isActive) {
            try {
                WikiProducer().run()
            } catch (e: CancellationException) {
                break
            } catch (e: Exception) {
                log.warn { "Producer error — reconnecting in 5s: ${e.message}" }
                delay(5_000)
            }
        }
    }

    // Start the Ktor REST API server
    val port = System.getenv("API_PORT")?.toInt() ?: 8090
    val server = embeddedServer(Netty, port = port) {
        install(ContentNegotiation) {
            json(Json {
                prettyPrint = true
                ignoreUnknownKeys = true
            })
        }
        install(CORS) {
            anyHost()
        }
        configureRoutes(kafkaStreams)
    }

    // Shutdown hook — ensures clean shutdown on Ctrl+C or SIGTERM
    Runtime.getRuntime().addShutdownHook(Thread {
        log.info { "Shutdown signal received" }
        producerJob.cancel()
        scope.cancel()
        server.stop(1_000, 5_000)
        streamsApp.stop()
        log.info { "Shutdown complete" }
    })
    log.info { "API server starting on port $port" }
    log.info { "Endpoints: /api/health  /api/stats  /api/hot-pages  /api/bot-ratio" }
    server.start(wait = true)
}
