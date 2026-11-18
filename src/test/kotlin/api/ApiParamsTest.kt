package api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ApiParamsTest {

    @Test
    fun `hot pages limit defaults to 20 when missing or not a number`() {
        assertEquals(20, parseHotPagesLimit(null))
        assertEquals(20, parseHotPagesLimit(""))
        assertEquals(20, parseHotPagesLimit("abc"))
    }

    @Test
    fun `hot pages limit within range is used as given`() {
        assertEquals(1, parseHotPagesLimit("1"))
        assertEquals(5, parseHotPagesLimit("5"))
        assertEquals(100, parseHotPagesLimit("100"))
    }

    @Test
    fun `hot pages limit is clamped to 1 through 100`() {
        assertEquals(1, parseHotPagesLimit("0"))
        assertEquals(1, parseHotPagesLimit("-3"))
        assertEquals(100, parseHotPagesLimit("101"))
        assertEquals(100, parseHotPagesLimit("100000"))
    }

    @Test
    fun `api port defaults to 8090 when unset or blank`() {
        assertEquals(8090, resolvePort(null))
        assertEquals(8090, resolvePort(""))
        assertEquals(8090, resolvePort("  "))
    }

    @Test
    fun `api port is parsed when valid`() {
        assertEquals(9000, resolvePort("9000"))
        assertEquals(9000, resolvePort(" 9000 "))
    }

    @Test
    fun `api port falls back to 8090 when not a valid port number`() {
        assertEquals(8090, resolvePort("abc"))
        assertEquals(8090, resolvePort("0"))
        assertEquals(8090, resolvePort("70000"))
    }
}
