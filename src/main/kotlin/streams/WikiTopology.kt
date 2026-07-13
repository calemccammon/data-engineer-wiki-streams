package streams

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import models.BotRatioState
import models.WikiEdit
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import java.time.Duration

private val log = KotlinLogging.logger {}

/**
 * Defines the Kafka Streams processing topology for the Wikipedia edit pipeline.
 *
 * Key concepts:
 *
 * KStream — an unbounded sequence of events (immutable log). Think of it as a
 *   stream of individual Wikipedia edits flowing through.
 *
 * KTable — a changelog-compacted table: only the latest value per key is kept.
 *   It represents *current state* rather than event history. Great for aggregations
 *   like "total edits for enwiki so far."
 *
 * Tumbling window — non-overlapping fixed-size time buckets. A 1-minute tumbling
 *   window produces one row per (key, minute). No event belongs to two windows.
 *   Used here for: edit counts per wiki per minute.
 *
 * Hopping window — overlapping windows defined by (size, advance). A 5-minute
 *   window advancing every 1 minute means each event appears in 5 windows.
 *   Used here for: "hot pages in the last 5 minutes," re-evaluated every minute.
 *
 * State store — persistent local storage (backed by RocksDB) that Kafka Streams
 *   manages. We can query these stores directly via the interactive queries API
 *   without touching Kafka or an external database.
 *
 * Topology built:
 *
 *   wiki.raw-edits (KStream)
 *       │
 *       ├─ filter(type == "edit" or "new", namespace == 0)
 *       │
 *       ├─ groupBy(wiki)
 *       │     └─ tumblingWindow(1 min) → count → KTable → wiki-edit-counts topic
 *       │
 *       ├─ groupBy(wiki + title)
 *       │     └─ hoppingWindow(5 min, advance 1 min) → count → KTable (hot-pages store)
 *       │
 *       └─ groupBy(wiki)
 *             └─ aggregate(BotRatioState) → KTable (bot-ratio store)
 */
object WikiTopology {

    // State store names — referenced by the Ktor API for interactive queries
    const val STORE_EDIT_COUNTS = "edit-counts-store"
    const val STORE_HOT_PAGES = "hot-pages-store"
    const val STORE_BOT_RATIO = "bot-ratio-store"

    // Output topics
    const val TOPIC_EDIT_STATS = "wiki.edit-stats"

    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private val stringSerde = Serdes.String()
    private val longSerde = Serdes.Long()

    fun build(inputTopic: String): Topology {
        val builder = StreamsBuilder()

        // ── Source stream ─────────────────────────────────────────────────────
        val rawEdits: KStream<String, String> = builder.stream(
            inputTopic,
            Consumed.with(stringSerde, stringSerde),
        )

        // Deserialize and filter to article-space edits/new pages only.
        // The key is already `wiki` (set by the producer).
        val edits: KStream<String, WikiEdit> = rawEdits
            .mapValues { raw -> tryParse(raw) }
            .filter { _, edit -> edit != null }
            .mapValues { edit -> edit!! }
            .filter { _, edit -> edit.type in setOf("edit", "new") && edit.namespace == 0 }

        // ── Branch 1: Tumbling 1-minute edit count per wiki ───────────────────
        //
        // Groups all edits by their Kafka key (which is the wiki name), then
        // counts them inside non-overlapping 1-minute time windows.
        //
        // Result: one row per (wiki, minute) telling you "enwiki had 47 edits
        // between 19:00 and 19:01."
        edits
            .groupByKey(Grouped.with(stringSerde, wikEditSerde()))
            .windowedBy(
                TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)),
            )
            .count(Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>(STORE_EDIT_COUNTS)
                .withKeySerde(stringSerde)
                .withValueSerde(longSerde))
            .toStream()
            .map { windowedKey: Windowed<String>, count: Long ->
                KeyValue(windowedKey.key(), count.toString())
            }
            .to(TOPIC_EDIT_STATS, Produced.with(stringSerde, stringSerde))

        // ── Branch 2: Hopping 5-minute hot pages ─────────────────────────────
        //
        // Re-keys the stream to (wiki|title), then counts inside hopping windows.
        // Each event falls into multiple windows (5 in total) — so a page edited
        // once shows up in 5 consecutive "last-5-minutes" snapshots.
        //
        // Result: at any given time you can query "which pages got the most edits
        // in the last 5 minutes?"
        edits
            .selectKey { _, edit -> "${edit.wiki}|${edit.title}" }
            .groupByKey(Grouped.with(stringSerde, wikEditSerde()))
            .windowedBy(
                TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30))
                    .advanceBy(Duration.ofMinutes(1)),
            )
            .count(Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>(STORE_HOT_PAGES)
                .withKeySerde(stringSerde)
                .withValueSerde(longSerde))

        // ── Branch 3: Running bot-vs-human ratio per wiki ────────────────────
        //
        // Uses a custom aggregation (not just count) to track both total edits
        // and bot edits in a single state object per wiki.
        //
        // Result: a KTable where each wiki key maps to its cumulative bot ratio.
        // Because this is a KTable (not windowed), it always reflects the latest
        // cumulative state — great for a "what % of all-time edits are bots?" metric.
        edits
            .groupByKey(Grouped.with(stringSerde, wikEditSerde()))
            .aggregate(
                { BotRatioState(wiki = "") },
                { wiki, edit, state -> state.copy(wiki = wiki).record(edit.bot) },
                Materialized.`as`<String, BotRatioState, KeyValueStore<Bytes, ByteArray>>(STORE_BOT_RATIO)
                    .withKeySerde(stringSerde)
                    .withValueSerde(botRatioSerde()),
            )

        return builder.build()
    }

    // ── Serdes ────────────────────────────────────────────────────────────────
    //
    // Kafka Streams needs serializers/deserializers for every type flowing through
    // the topology. Here we use Kotlinx.serialization + custom Serde wrappers.

    private fun wikEditSerde(): org.apache.kafka.common.serialization.Serde<WikiEdit> =
        kotlinxSerde()

    private fun botRatioSerde(): org.apache.kafka.common.serialization.Serde<BotRatioState> =
        kotlinxSerde()

    private inline fun <reified T : Any> kotlinxSerde(): org.apache.kafka.common.serialization.Serde<T> {
        // Capture the serializer while T is still reified (inline context)
        val kSerializer = kotlinx.serialization.serializer<T>()
        val serializer = object : org.apache.kafka.common.serialization.Serializer<T> {
            override fun serialize(topic: String, data: T?): ByteArray? =
                data?.let { json.encodeToString(kSerializer, it).toByteArray() }
        }
        val deserializer = object : org.apache.kafka.common.serialization.Deserializer<T> {
            override fun deserialize(topic: String, data: ByteArray?): T? =
                data?.let { json.decodeFromString(kSerializer, String(it)) }
        }
        return Serdes.serdeFrom(serializer, deserializer)
    }

    private fun tryParse(raw: String): WikiEdit? = try {
        json.decodeFromString<WikiEdit>(raw)
    } catch (e: Exception) {
        log.debug { "Unparseable message: ${e.message}" }
        null
    }
}
