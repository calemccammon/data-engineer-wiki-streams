package models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * A single event from the Wikimedia SSE stream (recentchange feed).
 *
 * The stream emits many event types; we primarily care about "edit" and "new"
 * (page creation), but we capture the full envelope to allow future filtering.
 *
 * Wikimedia SSE docs: https://stream.wikimedia.org/
 */
@Serializable
data class WikiEdit(
    /** Event type: "edit", "new", "log", "categorize", "external" */
    val type: String,

    /** Wiki database name, e.g. "enwiki", "dewiki", "commonswiki" */
    val wiki: String,

    /** Page title */
    val title: String,

    /** Editor username or IP address */
    val user: String,

    /** True if the edit was made by an automated bot account */
    val bot: Boolean,

    /** Unix timestamp of the edit */
    val timestamp: Long,

    /** Hostname of the MediaWiki server, e.g. "en.wikipedia.org" */
    @SerialName("server_name")
    val serverName: String,

    /** Namespace ID (0 = article, 4 = Wikipedia:, 1 = Talk:, etc.) */
    val namespace: Int = 0,

    /** Comment/edit summary left by the editor */
    val comment: String = "",

    /** Page length before and after the edit */
    val length: EditLength? = null,
)

@Serializable
data class EditLength(
    val old: Int? = null,
    val new: Int? = null,
) {
    /** Byte delta of the edit (+N or -N). Null if either side is unknown. */
    val delta: Int? get() = if (old != null && new != null) new - old else null
}

// ─── Aggregation result models ────────────────────────────────────────────────

/**
 * Aggregated edit statistics for a single wiki within a time window.
 * Written to the `wiki-edit-stats` Kafka topic by the Streams topology.
 */
@Serializable
data class EditStats(
    val wiki: String,
    val windowStartMs: Long,
    val windowEndMs: Long,
    val editCount: Long,
    val botEdits: Long,
    val humanEdits: Long,
    val uniqueEditors: Long,
) {
    val botRatio: Double get() = if (editCount > 0) botEdits.toDouble() / editCount else 0.0
}

/**
 * A single page's edit count within a hopping window — used to identify hot pages.
 */
@Serializable
data class PageEditCount(
    val wiki: String,
    val title: String,
    val windowStartMs: Long,
    val windowEndMs: Long,
    val editCount: Long,
)

/**
 * Running bot-vs-human ratio for a wiki — updated on every edit.
 * Stored in the `bot-ratio` state store.
 */
@Serializable
data class BotRatioState(
    val wiki: String,
    val totalEdits: Long = 0,
    val botEdits: Long = 0,
) {
    val humanEdits: Long get() = totalEdits - botEdits
    val botRatio: Double get() = if (totalEdits > 0) botEdits.toDouble() / totalEdits else 0.0

    fun record(isBot: Boolean): BotRatioState = copy(
        totalEdits = totalEdits + 1,
        botEdits = botEdits + (if (isBot) 1 else 0),
    )
}
