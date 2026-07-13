package streams

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import models.BotRatioState
import models.WikiEdit
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.apache.kafka.streams.state.QueryableStoreTypes
import java.time.Instant
import java.util.Properties

/**
 * Unit tests for the Kafka Streams topology using TopologyTestDriver.
 *
 * TopologyTestDriver is the key testing tool for Kafka Streams — it lets you:
 * - Feed records into input topics
 * - Advance the stream-time clock manually
 * - Query state stores directly
 * - Assert on output topics
 *
 * All of this works WITHOUT a running Kafka broker, making tests fast and hermetic.
 */
class WikiTopologyTest {

    private lateinit var driver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<String, String>
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    companion object {
        private const val INPUT_TOPIC = "wiki.raw-edits-test"
    }

    @BeforeEach
    fun setup() {
        val topology = WikiTopology.build(INPUT_TOPIC)
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "wiki-topology-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            // Use record-embedded timestamps (set by pipeInput) as the stream clock.
            // This is the default and makes windowed aggregations deterministic in tests.
        }
        driver = TopologyTestDriver(topology, props)
        inputTopic = driver.createInputTopic(
            INPUT_TOPIC,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
        )
    }

    @AfterEach
    fun teardown() {
        driver.close()
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun makeEdit(
        wiki: String = "enwiki",
        title: String = "Test article",
        user: String = "TestUser",
        bot: Boolean = false,
        type: String = "edit",
        namespace: Int = 0,
        timestamp: Long = Instant.now().epochSecond,
    ) = WikiEdit(
        type = type,
        wiki = wiki,
        title = title,
        user = user,
        bot = bot,
        timestamp = timestamp,
        serverName = "$wiki.wikipedia.org",
        namespace = namespace,
    )

    private fun send(edit: WikiEdit, timestampMs: Long = Instant.now().toEpochMilli()) {
        inputTopic.pipeInput(edit.wiki, json.encodeToString(edit), timestampMs)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    fun `edit count increments for each edit in the same window`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        repeat(5) { i ->
            send(makeEdit(wiki = "enwiki"), timestampMs = baseTime.plusSeconds(i * 10L).toEpochMilli())
        }

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_EDIT_COUNTS)
        val count = store.fetch("enwiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()

        assertNotNull(count)
        assertEquals(5L, count)
    }

    @Test
    fun `edits in different wikis are counted independently`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        send(makeEdit(wiki = "enwiki"), baseTime.toEpochMilli())
        send(makeEdit(wiki = "enwiki"), baseTime.plusSeconds(10).toEpochMilli())
        send(makeEdit(wiki = "dewiki"), baseTime.plusSeconds(20).toEpochMilli())

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_EDIT_COUNTS)

        val enCount = store.fetch("enwiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()
        val deCount = store.fetch("dewiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()

        assertEquals(2L, enCount, "enwiki should have 2 edits")
        assertEquals(1L, deCount, "dewiki should have 1 edit")
    }

    @Test
    fun `non-article namespace edits are filtered out`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        // namespace 0 = article (should pass)
        send(makeEdit(wiki = "enwiki", namespace = 0), baseTime.toEpochMilli())
        // namespace 4 = Wikipedia: project pages (should be filtered)
        send(makeEdit(wiki = "enwiki", namespace = 4), baseTime.plusSeconds(10).toEpochMilli())
        // namespace 1 = Talk: pages (should be filtered)
        send(makeEdit(wiki = "enwiki", namespace = 1), baseTime.plusSeconds(20).toEpochMilli())

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_EDIT_COUNTS)
        val count = store.fetch("enwiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()

        assertEquals(1L, count, "Only article-space edits should be counted")
    }

    @Test
    fun `log events are filtered out`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        send(makeEdit(wiki = "enwiki", type = "edit"), baseTime.toEpochMilli())
        send(makeEdit(wiki = "enwiki", type = "log"), baseTime.plusSeconds(10).toEpochMilli())
        send(makeEdit(wiki = "enwiki", type = "categorize"), baseTime.plusSeconds(20).toEpochMilli())

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_EDIT_COUNTS)
        val count = store.fetch("enwiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()

        assertEquals(1L, count, "Only edit/new events should pass the filter")
    }

    @Test
    fun `bot ratio accumulates correctly`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        // 3 human edits, 2 bot edits → bot ratio = 40%
        send(makeEdit(wiki = "enwiki", bot = false), baseTime.toEpochMilli())
        send(makeEdit(wiki = "enwiki", bot = false), baseTime.plusSeconds(10).toEpochMilli())
        send(makeEdit(wiki = "enwiki", bot = false), baseTime.plusSeconds(20).toEpochMilli())
        send(makeEdit(wiki = "enwiki", bot = true), baseTime.plusSeconds(30).toEpochMilli())
        send(makeEdit(wiki = "enwiki", bot = true), baseTime.plusSeconds(40).toEpochMilli())

        val store: ReadOnlyKeyValueStore<String, BotRatioState> = driver.getKeyValueStore(WikiTopology.STORE_BOT_RATIO)
        val state = store.get("enwiki")

        assertNotNull(state)
        assertEquals(5L, state.totalEdits)
        assertEquals(2L, state.botEdits)
        assertEquals(3L, state.humanEdits)
        assertEquals(0.4, state.botRatio, 0.001)
    }

    @Test
    fun `hot pages counts per page correctly`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        // Page A edited 3 times, page B once
        repeat(3) { i ->
            send(makeEdit(wiki = "enwiki", title = "PageA"), baseTime.plusSeconds(i * 30L).toEpochMilli())
        }
        send(makeEdit(wiki = "enwiki", title = "PageB"), baseTime.plusSeconds(90).toEpochMilli())

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_HOT_PAGES)
        val pageACount = store.fetch(
            "enwiki|PageA",
            baseTime.minusSeconds(1),
            baseTime.plusSeconds(300),
        ).asSequence().map { it.value }.maxOrNull()

        assertNotNull(pageACount)
        assertTrue(pageACount!! >= 3L, "PageA should have at least 3 edits")
    }

    @Test
    fun `malformed JSON events are silently dropped`() {
        val baseTime = Instant.parse("2024-01-15T10:00:00Z")

        // One valid edit
        send(makeEdit(wiki = "enwiki"), baseTime.toEpochMilli())
        // One invalid JSON event — should not crash the topology
        inputTopic.pipeInput("enwiki", "{ this is not valid json }", baseTime.plusSeconds(10).toEpochMilli())

        val store: ReadOnlyWindowStore<String, Long> = driver.getWindowStore(WikiTopology.STORE_EDIT_COUNTS)
        val count = store.fetch("enwiki", baseTime.minusSeconds(1), baseTime.plusSeconds(60))
            .asSequence().map { it.value }.maxOrNull()

        assertEquals(1L, count, "Invalid events should be dropped without crashing")
    }
}
