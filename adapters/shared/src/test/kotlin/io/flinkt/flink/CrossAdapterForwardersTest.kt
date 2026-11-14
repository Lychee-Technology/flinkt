package io.flinkt.flink

import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

/**
 * The cross-adapter comparison (docs/testing.md, "Generated forwarders"): the forwarders every adapter generated are
 * identical, parameter names and the `@Deprecated` and `@ExperimentalFlinkApi` annotations included, apart from the
 * per-line differences listed here.
 *
 * Generation passes when Flink changes a forwarded method's status, adds an overload or renames a parameter, so this
 * is where such a change between lines fails until it's listed (docs/architecture.md, "Drift"). The adapter's build
 * generates every adapter's forwarders and passes their directories, so each lane compares all the adapters, not only
 * its own.
 *
 * A member is compared as the generator wrote it: its annotations, its declaration and its body. A member that isn't
 * on every line is a difference on each line that has it, so a method whose status or parameter name differs between
 * lines is a difference on each of them. The listing names each difference by its view, its annotations and its
 * declaration, with the packages left out.
 */
class CrossAdapterForwardersTest {
    @Test
    fun `every adapter generates the same forwarders, apart from the listed per-line differences`() {
        val adapters = AdapterForwarders.read(ViewCodegen.adapterForwarders)
        assertEquals(PER_LINE_DIFFERENCES.keys, adapters.keys, "The listing has an entry for each adapter's line")
        assertTrue(ADAPTER_FLINK_LINE.toString() in adapters, "This adapter's own line, $ADAPTER_FLINK_LINE, is compared")
        // What is compared, so that the comparison can't pass on missing views or empty forward lists.
        for ((line, members) in adapters) {
            assertEquals(VIEWS, members.map { it.view }.toSortedSet(), line)
            assertTrue(members.any { "fun filter(" in it.text } && members.any { "fun name(" in it.text }, line)
        }

        val mismatches = mismatches(adapters)
        if (mismatches.isNotEmpty()) {
            fail(mismatches.joinToString("\n", prefix = "The adapters' forwarders differ from the listing:\n") { it.message })
        }
    }

    @Test
    fun `the comparison finds a status, a parameter name or a signature that one line changes`() {
        // The control for the test above: each edit makes one member on one line differ from the other lines'.
        val adapters = AdapterForwarders.read(ViewCodegen.adapterForwarders)
        val line = ADAPTER_FLINK_LINE.toString()
        val union = adapters.getValue(line).single { "fun union(" in it.text }
        val edits = listOf(
            "@io.flinkt.flink.ExperimentalFlinkApi\n" + union.text,
            "@Deprecated(\"Deprecated in Flink: DataStream.union\")\n" + union.text,
            union.text.replace("vararg streams:", "vararg others:").replace("streams.map", "others.map"),
            union.text.replace("FlinktDataStream<T>):", "FlinktDataStream<T>, slot: kotlin.Int):"),
        )
        for (edit in edits) {
            val edited = union.copy(text = edit)
            val changed = adapters + (line to adapters.getValue(line).map { if (it == union) edited else it })

            val expected = listOf("$line: unlisted ${edited.description}") +
                (adapters.keys - line).map { "$it: unlisted ${union.description}" }
            assertEquals(expected.sorted(), mismatches(changed).map { it.summary }.sorted(), edit)
        }
    }

    @Test
    fun `the comparison finds a listed difference that no longer differs`() {
        val adapters = AdapterForwarders.read(ViewCodegen.adapterForwarders)
        val line = PER_LINE_DIFFERENCES.entries.first { it.value.isNotEmpty() }.key
        val listed = adapters.getValue(line).first { it.description in PER_LINE_DIFFERENCES.getValue(line) }
        val others = adapters.filterKeys { it != line }.mapValues { (_, members) -> members + listed }

        assertEquals(listOf("$line: listed, but on every line: ${listed.description}"), mismatches(adapters + others).map { it.summary })
    }

    /**
     * Each difference between the adapters' forwarders that [PER_LINE_DIFFERENCES] doesn't list, and each listed one
     * that isn't a difference. Lists, not sets, so that two differences with one description need two entries.
     */
    private fun mismatches(adapters: Map<String, List<AdapterForwarders.Member>>): List<Mismatch> {
        val onEveryLine = adapters.values.map { it.toSet() }.reduce { common, members -> common intersect members }
        return adapters.flatMap { (line, members) ->
            val unmatched = PER_LINE_DIFFERENCES[line].orEmpty().toMutableList()
            val unlisted = members.filterNot { it in onEveryLine }.filterNot { unmatched.remove(it.description) }
            unlisted.map { Mismatch(line, it.description, it) } + unmatched.map { Mismatch(line, it, null) }
        }
    }

    /** A difference that isn't listed, with its [member], or a listed one that isn't a difference, without one. */
    private class Mismatch(val line: String, val description: String, val member: AdapterForwarders.Member?) {
        val summary: String = if (member != null) "$line: unlisted $description" else "$line: listed, but on every line: $description"

        /**
         * The [summary], and the member as the generator wrote it, packages included: two lines can differ only in the
         * package of a type, which the description leaves out.
         */
        val message: String = summary + member?.text?.lines()?.joinToString("") { "\n        $it" }.orEmpty()
    }

    private companion object {
        val VIEWS = sortedSetOf("FlinktDataStreamForwarders", "FlinktKeyedStreamForwarders", "FlinktSingleOutputStreamOperatorForwarders")

        /**
         * The forwarders that one line's adapter has and another's lacks or has with a difference, by line.
         *
         * On 1.20, these are Flink's own differences between the lines: overloads of forwarded methods that 2.x removed.
         * `partitionCustom` by field position or expression and `assignTimestampsAndWatermarks` with the old assigners
         * are deprecated, so they're forwarded with `@Deprecated`. `sinkTo` for the legacy `connector.sink.Sink` isn't
         * deprecated itself, so it's forwarded unannotated, as Flink declares it (docs/flink-compatibility.md,
         * "Differences between target lines").
         */
        val PER_LINE_DIFFERENCES: Map<String, List<String>> = mapOf(
            "2.3" to emptyList(),
            "1.20" to listOf(
                "FlinktDataStreamForwarders: @Deprecated(\"Deprecated in Flink: DataStream.partitionCustom\") " +
                    "fun <K> partitionCustom(partitioner: Partitioner<K>, field: Int): FlinktDataStream<T>",
                "FlinktDataStreamForwarders: @Deprecated(\"Deprecated in Flink: DataStream.partitionCustom\") " +
                    "fun <K> partitionCustom(partitioner: Partitioner<K>, field: String): FlinktDataStream<T>",
                "FlinktDataStreamForwarders: @Deprecated(\"Deprecated in Flink: DataStream.assignTimestampsAndWatermarks\") " +
                    "fun assignTimestampsAndWatermarks(timestampAndWatermarkAssigner: AssignerWithPeriodicWatermarks<T>): " +
                    "FlinktSingleOutputStreamOperator<T>",
                "FlinktDataStreamForwarders: @Deprecated(\"Deprecated in Flink: DataStream.assignTimestampsAndWatermarks\") " +
                    "fun assignTimestampsAndWatermarks(timestampAndWatermarkAssigner: AssignerWithPunctuatedWatermarks<T>): " +
                    "FlinktSingleOutputStreamOperator<T>",
                "FlinktDataStreamForwarders: fun sinkTo(sink: Sink<T, *, *, *>): DataStreamSink<T>",
                "FlinktDataStreamForwarders: fun sinkTo(sink: Sink<T, *, *, *>, " +
                    "customSinkOperatorUidHashes: CustomSinkOperatorUidHashes): DataStreamSink<T>",
            ),
        )
    }
}

/** The forwarders that `flinkt-view-codegen` generated in each adapter, read from the Kotlin it wrote. */
object AdapterForwarders {
    /**
     * One member of a generated `*Forwarders` interface, as the generator wrote it, without its indentation: its
     * annotations, its declaration and its body. The interface's own declaration is a member too, so a supertype that
     * differs between lines is a difference.
     */
    data class Member(val view: String, val text: String) {
        /** The view, the annotations and the declaration, without `public`, the body or packages. */
        val description: String
            get() {
                val lines = text.lines()
                val declaration = lines.indexOfFirst { it.startsWith("public ") }
                val signature = lines.take(declaration) +
                    lines[declaration].removePrefix("public ").removeSuffix(" =").removeSuffix(" {")
                return "$view: " + signature.joinToString(" ").replace(PACKAGE, "")
            }
    }

    /** The members of each adapter's forwarders, by the Flink line they were generated from, such as `2.3`. */
    fun read(directories: List<File>): Map<String, List<Member>> {
        val adapters = directories.map { directory ->
            val files = directory.walkTopDown().filter { it.isFile && it.name.endsWith("Forwarders.kt") }.toList()
            assertTrue(files.isNotEmpty(), "$directory has no generated forwarders")
            val parsed = files.map(::parse)
            val versions = parsed.map { it.first }.toSet()
            assertEquals(1, versions.size, "$directory was generated from $versions")
            line(versions.single()) to parsed.flatMap { it.second }
        }
        assertEquals(adapters.size, adapters.map { it.first }.toSet().size, "${adapters.map { it.first }}: one adapter per line")
        return adapters.toMap()
    }

    /** The Flink version a generated file names in its first line, and the file's members. */
    private fun parse(file: File): Pair<String, List<Member>> {
        val lines = file.readLines()
        val version = HEADER.matchEntire(lines.first())?.groupValues?.get(1)
            ?: error("$file doesn't start with flinkt-view-codegen's header: ${lines.first()}")
        val start = lines.indexOfFirst { it.startsWith("public sealed interface ") }
        check(start >= 0 && lines.last() == "}") { "$file isn't a generated forwarders interface" }
        val view = file.name.removeSuffix(".kt")
        val blocks = mutableListOf(mutableListOf(lines[start]))
        for (line in lines.subList(start + 1, lines.size - 1)) {
            if (line.isBlank()) blocks += mutableListOf<String>() else blocks.last() += line.trim()
        }
        return version to blocks.filter { it.isNotEmpty() }.map { Member(view, it.joinToString("\n")) }
    }

    /** `2.3` for `2.3.0`. */
    private fun line(version: String): String = version.split('.').take(2).joinToString(".")

    private val HEADER = Regex("// Generated by flinkt-view-codegen from \\S+, Flink (\\S+)\\. Don't edit it\\.")

    /** A qualifier, such as `org.apache.flink.api.common.functions.` before `Partitioner`. */
    private val PACKAGE = Regex("\\b(?:[a-z_][a-z0-9_]*\\.)+(?=[A-Z])")
}
