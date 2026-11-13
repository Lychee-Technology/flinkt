package io.flinkt.codegen

/**
 * One public instance method of a Flink class, with the parameter names its class file records.
 *
 * [className] is the binary name of the class that declares the method, such as
 * `org.apache.flink.streaming.api.datastream.DataStream`, with `$` before a nested class's name. [descriptor] is the
 * class file's JVM descriptor, which tells the method's overloads apart.
 */
internal data class FlinkMethod(
    val className: String,
    val name: String,
    val descriptor: String,
    val parameterNames: List<String>,
) {
    init {
        val parameters = JvmDescriptors.parameters(descriptor).size
        require(parameterNames.size == parameters) {
            "$className.$name$descriptor has $parameters parameters, but ${parameterNames.size} names: $parameterNames"
        }
    }
}

/**
 * The names file, `flink-api.tsv`: the parameter names of Flink's public methods, read from the class files of one
 * Flink release, which it records. Each adapter checks one in, and the generator looks each forwarded method up in it
 * by its declaring class, name and descriptor ([find]).
 *
 * The file is text, so a diff shows each name that changes:
 *
 * ```text
 * # comment lines
 * flink-version	2.3.0
 * org.apache.flink.streaming.api.datastream.DataStream	union	streams	([Lorg/apache/flink/streaming/api/datastream/DataStream;)Lorg/apache/flink/streaming/api/datastream/DataStream;
 * ```
 *
 * Each row has four tab-separated columns: the class, the method, its parameter names separated by commas (empty for
 * none), and the descriptor, which comes last so that a row has no trailing whitespace. Rows are sorted.
 */
internal class FlinkApi(val flinkVersion: String, methods: Collection<FlinkMethod>) {
    val methods: List<FlinkMethod> = methods.sortedWith(compareBy({ it.className }, { it.name }, { it.descriptor }))

    private val byKey: Map<String, FlinkMethod> = this.methods.associateBy { key(it.className, it.name, it.descriptor) }

    init {
        require(flinkVersion.isNotBlank() && flinkVersion.none { it.isWhitespace() }) { "Not a Flink version: '$flinkVersion'" }
        require(byKey.size == this.methods.size) {
            "A method is listed twice: " +
                this.methods.groupBy { key(it.className, it.name, it.descriptor) }.filterValues { it.size > 1 }.keys.first()
        }
    }

    fun find(className: String, name: String, descriptor: String): FlinkMethod? = byKey[key(className, name, descriptor)]

    fun write(): String = buildString {
        HEADER.forEach { append("# ").append(it).append('\n') }
        append(VERSION_KEY).append('\t').append(flinkVersion).append('\n')
        for (method in methods) {
            append(method.className).append('\t')
                .append(method.name).append('\t')
                .append(method.parameterNames.joinToString(",")).append('\t')
                .append(method.descriptor).append('\n')
        }
    }

    override fun equals(other: Any?): Boolean =
        other is FlinkApi && other.flinkVersion == flinkVersion && other.methods == methods

    override fun hashCode(): Int = 31 * flinkVersion.hashCode() + methods.hashCode()

    companion object {
        private const val VERSION_KEY = "flink-version"

        private val HEADER = listOf(
            "The parameter names of Flink's public instance methods, read from Flink's class files by flinkt-view-codegen.",
            "The view forwarders take their parameter names from this file. Don't edit it: extract it again with the",
            "adapter's extractFlinkApi task. Columns: class, method, parameter names, JVM descriptor.",
        )

        /** Parses a names file. [source] names it in error messages. */
        fun parse(text: String, source: String): FlinkApi {
            var version: String? = null
            val methods = mutableListOf<FlinkMethod>()
            text.lineSequence().forEachIndexed { index, line ->
                if (line.isEmpty() || line.startsWith('#')) return@forEachIndexed
                fun malformed(reason: String): Nothing =
                    throw IllegalArgumentException("$source:${index + 1}: $reason: $line")
                val columns = line.split('\t')
                if (version == null) {
                    if (columns.size != 2 || columns[0] != VERSION_KEY) malformed("the first row must be '$VERSION_KEY<tab><version>'")
                    version = columns[1]
                    return@forEachIndexed
                }
                if (columns.size != 4) malformed("a row has 4 tab-separated columns")
                val (className, name, names, descriptor) = columns
                try {
                    methods += FlinkMethod(className, name, descriptor, if (names.isEmpty()) emptyList() else names.split(','))
                } catch (e: IllegalArgumentException) {
                    malformed(e.message ?: "malformed row")
                }
            }
            val fileVersion = version ?: throw IllegalArgumentException("$source has no '$VERSION_KEY' row")
            return try {
                FlinkApi(fileVersion, methods)
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("$source: ${e.message}", e)
            }
        }

        private fun key(className: String, name: String, descriptor: String) = "$className#$name$descriptor"
    }
}
