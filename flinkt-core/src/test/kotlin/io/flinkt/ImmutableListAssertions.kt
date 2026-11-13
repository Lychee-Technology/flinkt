package io.flinkt

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.assertThrows

/**
 * Asserts that [list], which must have an element, can't be changed even by a caller who casts it to `MutableList`,
 * as Java code and an unchecked cast can. Each attempt fails, and the list keeps its elements.
 */
internal fun <T> assertImmutable(list: List<T>) {
    assertFalse(list.isEmpty(), "the test needs a list with an element")
    val before = ArrayList(list)
    @Suppress("UNCHECKED_CAST")
    val mutable = list as MutableList<T>
    val first = list.first()

    assertThrows<UnsupportedOperationException> { mutable.add(first) }
    assertThrows<UnsupportedOperationException> { mutable.add(0, first) }
    assertThrows<UnsupportedOperationException> { mutable.addAll(listOf(first)) }
    assertThrows<UnsupportedOperationException> { mutable.set(0, first) }
    assertThrows<UnsupportedOperationException> { mutable.removeAt(0) }
    assertThrows<UnsupportedOperationException> { mutable.remove(first) }
    assertThrows<UnsupportedOperationException> { mutable.clear() }
    assertThrows<UnsupportedOperationException> { mutable.iterator().apply { next() }.remove() }
    assertThrows<UnsupportedOperationException> { mutable.listIterator().apply { next() }.set(first) }
    assertThrows<UnsupportedOperationException> { mutable.subList(0, 1).clear() }
    assertEquals(before, list)
}
