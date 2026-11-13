package io.flinkt.flink

import io.flinkt.flink.ClassFileReader.Call
import java.lang.reflect.GenericArrayType
import java.lang.reflect.Method
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.TypeVariable
import java.lang.reflect.WildcardType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The views' structural tests (docs/testing.md, "Leaving the view is explicit", "Operators take their result type
 * from the call site" and "Flink adapter contract"), on the adapter's own Flink line: they read the compiled views,
 * and Flink's class files.
 */
class ViewStructureTest {
    /**
     * The view classes: what `.flinkt()` returns, and each class of the adapter's package that a public method of a
     * view returns, transitively.
     */
    private val views: List<Class<*>> = run {
        val entry = Class.forName("io.flinkt.flink.FlinktKt").methods.filter { it.name == "flinkt" }.map { it.returnType }
        val found = linkedSetOf<Class<*>>()
        val queue = ArrayDeque(entry)
        while (queue.isNotEmpty()) {
            val view = queue.removeFirst()
            if (!found.add(view)) continue
            queue += view.methods.map { it.returnType }.filter { it.`package` == FlinktDataStream::class.java.`package` }
        }
        found.toList()
    }

    @Test
    fun `the views are the three that enter`() {
        assertEquals(
            setOf(FlinktDataStream::class.java, FlinktSingleOutputStreamOperator::class.java, FlinktKeyedStream::class.java),
            views.toSet(),
        )
    }

    @Test
    fun `no view class extends or implements a Flink type`() {
        for (view in views) {
            val supertypes = supertypes(view)
            assertTrue(supertypes.any { it.simpleName == "FlinktDataStreamForwarders" }, "$view: $supertypes")
            assertEquals(emptyList<Class<*>>(), supertypes.filter { it.name.startsWith("org.apache.flink.") }, view.name)
        }
        // The control: the check sees a Flink class's Flink supertype.
        assertTrue(DataStream::class.java in supertypes(SingleOutputStreamOperator::class.java))
    }

    @Test
    fun `each public view method that introduces a type parameter in its result is inline with it reified, or takes a TypeInformation for it`() {
        val found = sortedSetOf<String>()
        for (view in views) {
            for (method in view.methods) {
                if (method.isBridge || method.declaringClass == Any::class.java) continue
                for (parameter in method.typeParameters.filter { method.genericReturnType.mentions(it) }) {
                    val reified = parameter.name in ClassFileReader.reifiedTypeParameters(method)
                    val explicit = method.genericParameterTypes.any { it.isTypeInformationOf(parameter) }
                    val describe = describe(method)
                    assertTrue(reified || explicit, "$describe: ${parameter.name} is neither reified nor given by a TypeInformation")
                    found += "$describe introduces ${parameter.name}, " + if (reified) "reified" else "from a TypeInformation"
                }
            }
        }
        // The views' type-introducing operators, so that the test can't pass by finding none.
        assertEquals(
            sortedSetOf(
                "FlinktDataStream.keyBy(KeySelector) introduces K, reified",
                "FlinktDataStream.keyBy(KeySelector, TypeInformation) introduces K, from a TypeInformation",
                "FlinktDataStream.map(MapFunction) introduces R, reified",
                "FlinktDataStream.map(MapFunction, TypeInformation) introduces R, from a TypeInformation",
                "FlinktKeyedStream.process(KeyedProcessFunction) introduces R, reified",
                "FlinktKeyedStream.process(KeyedProcessFunction, TypeInformation) introduces R, from a TypeInformation",
            ),
            found,
        )
    }

    @Test
    fun `hand-written adapter code calls only these Flink methods, each @Public or @PublicEvolving on this line`() {
        // The adapter's classes, without the forwarders flinkt-view-codegen generates, which it checks itself.
        val classes = ClassFileReader.classesBeside(FlinktDataStream::class.java)
            .filterNot { it.substringAfterLast('/').substringBefore('$').endsWith("Forwarders") }
        assertTrue("io/flinkt/flink/FlinktKeyedStream" in classes, classes.toString())
        val calls = classes.flatMap { ClassFileReader.calls(ClassFileReader.read(it), "org/apache/flink/") }.toSet()

        val statuses = calls.associate { call -> call.toString() to stability(call) }
        assertEquals(
            sortedMapOf(
                "DataStream.keyBy(KeySelector, TypeInformation)" to "@Public",
                "DataStream.map(MapFunction, TypeInformation)" to "@Public",
                "FlinkVersion.current()" to "@Public",
                "KeyedStream.process(KeyedProcessFunction)" to "@PublicEvolving",
                "SingleOutputStreamOperator.returns(TypeInformation)" to "@Public",
            ),
            statuses.toSortedMap(),
        )
    }

    @Test
    fun `the stability check refuses an @Internal method and an @Experimental one`() {
        // The control for the test above, on methods hand-written code doesn't call.
        assertEquals("@Internal", stability(call("org/apache/flink/streaming/api/datastream/DataStream", "getTransformation")))
        assertEquals(
            "@Experimental",
            stability(call("org/apache/flink/streaming/api/datastream/DataStreamUtils", "reinterpretAsKeyedStream")),
        )
    }

    /**
     * The stability that Flink declares for the method [call] reaches: the method's own annotation, or else its
     * declaring class's. `@Internal` and `@Experimental` win wherever they are. Read from the class files, where 1.20
     * keeps its class-retention annotations and 2.x its runtime ones.
     */
    private fun stability(call: Call): String {
        val (owner, method) = ClassFileReader.resolve(call)
        assertTrue(ClassFileReader.isPublic(method), "$call isn't public")
        val onMethod = ClassFileReader.annotations(method).mapNotNull(STABILITY::get)
        val onClass = ClassFileReader.annotations(owner).mapNotNull(STABILITY::get)
        return listOf("@Internal", "@Experimental").firstOrNull { it in onMethod || it in onClass }
            ?: onMethod.singleOrNull()
            ?: onClass.singleOrNull()
            ?: "no stability annotation"
    }

    /** A call to the first method named [name] that [owner] declares. */
    private fun call(owner: String, name: String): Call {
        val method = ClassFileReader.read(owner).methods.first { it.name == name }
        return Call(owner, name, method.desc)
    }

    private fun supertypes(type: Class<*>): Set<Class<*>> {
        val found = linkedSetOf<Class<*>>()
        val queue = ArrayDeque(listOfNotNull(type.superclass) + type.interfaces)
        while (queue.isNotEmpty()) {
            val supertype = queue.removeFirst()
            if (found.add(supertype)) queue += listOfNotNull(supertype.superclass) + supertype.interfaces
        }
        return found
    }

    private fun Type.mentions(parameter: TypeVariable<*>): Boolean = when (this) {
        is TypeVariable<*> -> this == parameter
        is ParameterizedType -> actualTypeArguments.any { it.mentions(parameter) }
        is WildcardType -> (upperBounds + lowerBounds).any { it.mentions(parameter) }
        is GenericArrayType -> genericComponentType.mentions(parameter)
        else -> false
    }

    private fun Type.isTypeInformationOf(parameter: TypeVariable<*>): Boolean =
        this is ParameterizedType && rawType == TypeInformation::class.java && actualTypeArguments.single() == parameter

    private fun describe(method: Method): String =
        method.declaringClass.simpleName + "." + method.name + method.parameterTypes.joinToString(", ", "(", ")") { it.simpleName }

    private companion object {
        val STABILITY: Map<String, String> = listOf("Public", "PublicEvolving", "Experimental", "Internal")
            .associateBy({ "Lorg/apache/flink/annotation/$it;" }, { "@$it" })
    }
}
