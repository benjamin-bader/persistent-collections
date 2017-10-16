package com.bendb.kotlin.persistent

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class PersistentListTest {
    @Test fun `empty list has no elements`() {
        assertEquals(0, emptyPersistentList<Int>().size)
    }

    @Test fun `empty list is empty`() {
        assertTrue(emptyPersistentList<Int>().isEmpty())
    }

    @Test fun `list size`() {
        val list = persistentListOf(1, 2, 3)
        assertEquals(3, list.size)
        assertEquals(listOf(1, 2, 3), list.iterator().asSequence().toList())
    }

    @Test fun `list with 33 elements`() {
        val numbers = (1..33).toList()
        val list = persistentListOf(*(numbers.toTypedArray()))

        assertEquals(33, list.size)
        assertEquals(numbers, list.asSequence().toList())
    }

    @Test fun `set works`() {
        val numbers = (1..64).toMutableList()
        var list = persistentListOf(*(numbers.toTypedArray()))

        numbers[60] = 100
        list = list.set(60, 100)

        assertEquals(numbers, list.asSequence().toList())

    }
}