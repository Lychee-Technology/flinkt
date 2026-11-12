package io.flinkt

import java.util.Collections

/**
 * A copy of this list that nobody can change: not the caller who passed the original, and not a caller who casts the
 * copy to `MutableList`. Kotlin's `toList()` and `map()` can return a `java.util.ArrayList`, which such a cast can
 * change.
 */
internal fun <T> List<T>.immutableCopy(): List<T> = Collections.unmodifiableList(ArrayList(this))
