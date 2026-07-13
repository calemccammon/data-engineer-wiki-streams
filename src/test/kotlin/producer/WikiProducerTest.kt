package producer

import kotlinx.serialization.json.Json
import models.WikiEdit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Unit tests for WikiProducer filtering and parsing logic.
 *
 * These tests exercise the decisions the producer makes about which SSE events
 * to forward to Kafka — without needing a real SSE connection or Kafka broker.
 */
class WikiProducerTest {

    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun makeRaw(
        type: String = "edit",
        wiki: String = "enwiki",
        title: String = "Test article",
        namespace: Int = 0,
        bot: Boolean = false,
        user: String = "TestUser",
    ) = """
        {
          "type": "$type",
          "wiki": "$wiki",
          "title": "$title",
          "user": "$user",
          "bot": $bot,
          "timestamp": 1705312800,
          "server_name": "$wiki.wikipedia.org",
          "namespace": $namespace,
          "comment": "test edit"
        }
    """.trimIndent()

    private fun shouldForward(raw: String): Boolean {
        val edit = try {
            json.decodeFromString<WikiEdit>(raw)
        } catch (e: Exception) {
            return false
        }
        return edit.type in setOf("edit", "new") && edit.namespace == 0
    }

    // ── Parsing tests ─────────────────────────────────────────────────────────

    @Test
    fun `valid edit event is parsed correctly`() {
        val raw = makeRaw(type = "edit", wiki = "enwiki", title = "Quantum mechanics", namespace = 0)
        val edit = json.decodeFromString<WikiEdit>(raw)

        assertEquals("edit", edit.type)
        assertEquals("enwiki", edit.wiki)
        assertEquals("Quantum mechanics", edit.title)
        assertEquals(0, edit.namespace)
        assertFalse(edit.bot)
    }

    @Test
    fun `bot flag is parsed correctly`() {
        val botRaw = makeRaw(bot = true)
        val humanRaw = makeRaw(bot = false)

        assertTrue(json.decodeFromString<WikiEdit>(botRaw).bot)
        assertFalse(json.decodeFromString<WikiEdit>(humanRaw).bot)
    }

    @Test
    fun `unknown extra fields are ignored`() {
        val raw = """
            {
              "type": "edit",
              "wiki": "enwiki",
              "title": "Test",
              "user": "Alice",
              "bot": false,
              "timestamp": 1705312800,
              "server_name": "en.wikipedia.org",
              "namespace": 0,
              "unknown_future_field": "some value",
              "another_unknown": 42
            }
        """.trimIndent()

        val edit = json.decodeFromString<WikiEdit>(raw)
        assertEquals("enwiki", edit.wiki)
    }

    // ── Filtering tests ───────────────────────────────────────────────────────

    @Test
    fun `article-space edit events are forwarded`() {
        assertTrue(shouldForward(makeRaw(type = "edit", namespace = 0)))
    }

    @Test
    fun `new page events are forwarded`() {
        assertTrue(shouldForward(makeRaw(type = "new", namespace = 0)))
    }

    @Test
    fun `log events are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "log", namespace = 0)))
    }

    @Test
    fun `categorize events are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "categorize", namespace = 0)))
    }

    @Test
    fun `external events are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "external", namespace = 0)))
    }

    @Test
    fun `talk page edits (namespace 1) are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "edit", namespace = 1)))
    }

    @Test
    fun `wikipedia project pages (namespace 4) are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "edit", namespace = 4)))
    }

    @Test
    fun `user pages (namespace 2) are filtered out`() {
        assertFalse(shouldForward(makeRaw(type = "edit", namespace = 2)))
    }

    @Test
    fun `bot edits in article space are forwarded (bots tracked for ratio)`() {
        // Bot edits are forwarded — we want to count them for the bot ratio metric
        assertTrue(shouldForward(makeRaw(type = "edit", namespace = 0, bot = true)))
    }
}
