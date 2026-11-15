package io.flinkt.flink.format

import java.io.ByteArrayOutputStream
import org.apache.flink.core.memory.DataOutputSerializer
import org.apache.flink.core.memory.DataOutputView
import org.apache.flink.core.memory.MemorySegment
import org.apache.flink.core.memory.MemorySegmentFactory
import org.apache.flink.runtime.memory.AbstractPagedOutputView
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * A paged view: what Flink's sort buffers write records to. Its pages are small, so multi-byte writes cross them.
 * `AbstractPagedOutputView.writeFloat` and `writeDouble` keep a NaN's raw bits, where `DataOutputSerializer`'s
 * canonicalize it (docs/architecture.md, "Generated code and the adapter").
 */
class CollectingPagedOutputView(private val pageSize: Int = 7) :
    AbstractPagedOutputView(MemorySegmentFactory.allocateUnpooledSegment(pageSize), pageSize, 0) {
    private val full = ArrayList<MemorySegment>()

    override fun nextSegment(current: MemorySegment, positionInCurrent: Int): MemorySegment {
        full += current
        return MemorySegmentFactory.allocateUnpooledSegment(pageSize)
    }

    fun bytes(): ByteArray {
        val bytes = ByteArrayOutputStream()
        val page = ByteArray(pageSize)
        for (segment in full) {
            segment.get(0, page, 0, pageSize)
            bytes.write(page, 0, pageSize)
        }
        currentSegment.get(0, page, 0, currentPositionInSegment)
        bytes.write(page, 0, currentPositionInSegment)
        return bytes.toByteArray()
    }
}

/** What [write] writes to a `DataOutputSerializer` and to a paged view. */
class Written(val serializer: ByteArray, val paged: ByteArray) {
    /** Asserts that both views got the same bytes, and returns them as hex. */
    fun hex(): String {
        assertEquals(hex(serializer), hex(paged), "a DataOutputSerializer and a paged view got different bytes")
        return hex(serializer)
    }

    val bytes: ByteArray get() = serializer.also { hex() }
}

fun written(write: (DataOutputView) -> Unit): Written {
    val serializer = DataOutputSerializer(4)
    write(serializer)
    val paged = CollectingPagedOutputView()
    write(paged)
    return Written(serializer.copyOfBuffer, paged.bytes())
}

/** Bytes as lower-case hex pairs separated by spaces, such as `00 2a`. */
fun hex(bytes: ByteArray): String = bytes.joinToString(" ") { "%02x".format(it) }

/** The bytes that [hex] spells, ignoring spaces, line breaks and `|` separators. */
fun bytes(hex: String): ByteArray {
    val digits = hex.filter { !it.isWhitespace() && it != '|' }
    require(digits.length % 2 == 0) { "odd number of hex digits in $hex" }
    return ByteArray(digits.length / 2) { digits.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
}

/** [hex] with spaces only, for comparing with [hex] of some bytes. */
fun normalized(hex: String): String = hex(bytes(hex))

/** The hex pairs that start each line of [annotated], without the comment that follows them on the line. */
fun annotatedHex(annotated: String): String = annotated.lines().joinToString(" ") { line ->
    line.trim().split(Regex("\\s+")).takeWhile { it == "|" || it.matches(Regex("[0-9a-f]{2}")) }.joinToString(" ")
}
