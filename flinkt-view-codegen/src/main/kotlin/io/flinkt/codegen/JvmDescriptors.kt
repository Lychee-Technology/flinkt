package io.flinkt.codegen

/** JVM method descriptors, such as `(I[Ljava/lang/String;)V`, as the class file and the names file write them. */
internal object JvmDescriptors {
    /** The descriptor of each parameter of the method [descriptor], in order. */
    fun parameters(descriptor: String): List<String> {
        require(descriptor.startsWith('(')) { "Not a method descriptor: $descriptor" }
        val end = descriptor.indexOf(')')
        require(end > 0) { "Not a method descriptor: $descriptor" }
        val parameters = mutableListOf<String>()
        var i = 1
        while (i < end) {
            val start = i
            while (descriptor[i] == '[') i++
            if (descriptor[i] == 'L') {
                i = descriptor.indexOf(';', i)
                require(i in 0 until end) { "Not a method descriptor: $descriptor" }
            } else {
                require(descriptor[i] in PRIMITIVES) { "Not a method descriptor: $descriptor" }
            }
            i++
            parameters += descriptor.substring(start, i)
        }
        return parameters
    }

    /** The local-variable slots a parameter of type [parameter] takes: two for `long` and `double`, else one. */
    fun slots(parameter: String): Int = if (parameter == "J" || parameter == "D") 2 else 1

    /**
     * Corrects the descriptor that KSP's `Resolver.mapToJvmSignature` gives a Java varargs method.
     *
     * The class file declares a varargs parameter `T... values` as the array `T[]`, but `mapToJvmSignature` omits that
     * array marker, so `union(DataStream<T>... streams)` comes back as `(Lorg/apache/flink/…/DataStream;)…` instead of
     * the class file's `([Lorg/apache/flink/…/DataStream;)…`. [elementArrayDimensions] is the number of array markers of
     * the vararg's element type, which is 0 unless it's an array itself. If KSP's last parameter has that many markers,
     * KSP dropped the varargs marker, and this adds it back. If it has one more, the descriptor is already right.
     */
    fun withVarargsArray(descriptor: String, elementArrayDimensions: Int): String {
        val parameters = parameters(descriptor)
        require(parameters.isNotEmpty()) { "A varargs method has a parameter: $descriptor" }
        val last = parameters.last()
        val dimensions = last.takeWhile { it == '[' }.length
        val fixedLast = when (dimensions) {
            elementArrayDimensions -> "[$last"
            elementArrayDimensions + 1 -> last
            else -> throw IllegalArgumentException(
                "The varargs parameter of $descriptor doesn't have $elementArrayDimensions or ${elementArrayDimensions + 1} array dimensions",
            )
        }
        return "(" + (parameters.dropLast(1) + fixedLast).joinToString("") + descriptor.substring(descriptor.indexOf(')'))
    }

    private const val PRIMITIVES = "ZBCSIJFD"
}
