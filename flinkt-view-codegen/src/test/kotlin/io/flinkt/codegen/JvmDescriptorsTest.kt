package io.flinkt.codegen

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class JvmDescriptorsTest {
    @Test
    fun `a descriptor splits into its parameters' descriptors`() {
        assertEquals(
            listOf("I", "[J", "Ljava/lang/String;", "[[Lorg/example/Outer\$Inner;", "D"),
            JvmDescriptors.parameters("(I[JLjava/lang/String;[[Lorg/example/Outer\$Inner;D)V"),
        )
        assertEquals(emptyList<String>(), JvmDescriptors.parameters("()Ljava/util/List;"))
    }

    @Test
    fun `a malformed descriptor fails`() {
        for (descriptor in listOf("I", "(I", "(Q)V", "(Ljava/lang/String)V", "([)V")) {
            assertThrows<IllegalArgumentException>(descriptor) { JvmDescriptors.parameters(descriptor) }
        }
    }

    @Test
    fun `a long or a double takes two local-variable slots`() {
        assertEquals(listOf(1, 2, 2, 1, 1), listOf("I", "J", "D", "[J", "Ljava/lang/Long;").map(JvmDescriptors::slots))
    }

    @Test
    fun `the varargs array marker that KSP drops is put back`() {
        // KSP describes union(DataStream<T>... streams) without the array marker of the class file.
        assertEquals(
            "([Lorg/apache/flink/streaming/api/datastream/DataStream;)Lorg/apache/flink/streaming/api/datastream/DataStream;",
            JvmDescriptors.withVarargsArray(
                "(Lorg/apache/flink/streaming/api/datastream/DataStream;)Lorg/apache/flink/streaming/api/datastream/DataStream;",
                elementArrayDimensions = 0,
            ),
        )
        assertEquals("(Ljava/lang/String;[I)V", JvmDescriptors.withVarargsArray("(Ljava/lang/String;I)V", elementArrayDimensions = 0))
        // String[]... values: the element is an array itself.
        assertEquals("([[Ljava/lang/String;)V", JvmDescriptors.withVarargsArray("([Ljava/lang/String;)V", elementArrayDimensions = 1))
    }

    @Test
    fun `a varargs descriptor that has its array marker is kept`() {
        assertEquals("(I[I)V", JvmDescriptors.withVarargsArray("(I[I)V", elementArrayDimensions = 0))
        assertEquals("([[Ljava/lang/String;)V", JvmDescriptors.withVarargsArray("([[Ljava/lang/String;)V", elementArrayDimensions = 1))
    }

    @Test
    fun `a varargs descriptor that matches neither fails`() {
        assertThrows<IllegalArgumentException> { JvmDescriptors.withVarargsArray("([[I)V", elementArrayDimensions = 0) }
        assertThrows<IllegalArgumentException> { JvmDescriptors.withVarargsArray("()V", elementArrayDimensions = 0) }
    }
}
