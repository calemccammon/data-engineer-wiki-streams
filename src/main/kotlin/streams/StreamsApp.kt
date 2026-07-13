package streams

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import java.util.Properties

private val log = KotlinLogging.logger {}

class StreamsApp(
    private val bootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092",
    private val appId: String = System.getenv("KAFKA_STREAMS_APP_ID") ?: "wiki-streams-processor",
    private val inputTopic: String = System.getenv("KAFKA_TOPIC_RAW_EDITS") ?: "wiki.raw-edits",
    private val stateDir: String = System.getenv("STATE_DIR")
        ?: (System.getProperty("java.io.tmpdir") + "/kafka-streams/wiki-streams"),
) {
    private lateinit var _streams: KafkaStreams

    fun start(): KafkaStreams {
        val topology = WikiTopology.build(inputTopic)
        log.info { "Topology description:\n${topology.describe()}" }

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        }

        _streams = KafkaStreams(topology, props)

        _streams.setUncaughtExceptionHandler { ex ->
            log.error(ex) { "Uncaught exception in Kafka Streams thread" }
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
        }

        _streams.setStateListener { newState, oldState ->
            log.info { "Streams state: $oldState → $newState" }
        }

        _streams.start()
        log.info { "Kafka Streams started (app.id=$appId)" }
        return _streams
    }

    fun getStreams(): KafkaStreams = _streams

    fun stop() {
        if (::_streams.isInitialized) {
            _streams.close()
            log.info { "Kafka Streams stopped" }
        }
    }
}
