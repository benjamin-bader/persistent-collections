package com.bendb.kotlin.persistent

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.Random
import kotlin.math.exp

class ArrayPersistentMapTest {
    @Test fun `empty maps are empty`() {
        assertEquals(0, ArrayPersistentMap.empty<Unit, Unit>().size)
    }

    @Test fun `put semantics hold`() {
        val map = ArrayPersistentMap.empty<String, String>()
                .put("foo", "bar")
                .put("foo", "bazzz")

        assertEquals("bazzz", map.get("foo"))
    }
}

class HashPersistentMapTest {
    @Test fun `empty maps are empty`() {
        assertEquals(0, HashPersistentMap.empty<Unit, Unit>().size)
    }

    @Test fun `put semantics hold`() {
        val map = HashPersistentMap.empty<String, String>()
                .put("foo", "bar")
                .put("foo", "bazzz")

        assertEquals("bazzz", map.get("foo"))
    }

    @Test fun `stress BitmapNode`() {
        val random = Random(42)
        val numPairs = 8192
        var values = (1..numPairs).map { i -> ((i * 2) - 1) to (i * 2) }

        var map: PersistentMap<Int, Int> = HashPersistentMap.empty()
        for (iteration in 1..100) {
            //println("iteration $iteration")
            var expectedSize = 0

            values = values.shuffled(random)
            for ((key, value) in values) {
                //println("put: $key -> $value")
                map = map.put(key, value)
                assertEquals(++expectedSize, map.size)
            }

            assertEquals(numPairs, map.size)
            assertFalse(map.isEmpty())
            for (i in 1..numPairs step 2) {
                assertEquals(i + 1, map.get(i))
            }

            values = values.shuffled(random)
            for ((key, _) in values) {
                //println("remove: $key")
                map = map.remove(key)
                assertEquals(--expectedSize, map.size)
            }

            assertEquals(0, map.size)
            assertTrue(map.isEmpty())
        }
    }
}
