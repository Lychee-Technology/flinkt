plugins { kotlin("jvm") }
kotlin { jvmToolchain(25) }
dependencies { implementation("com.github.javaparser:javaparser-symbol-solver-core:3.28.2") }

// One extraction per Flink line: the sources jar that holds the stream classes, plus that line's binary
// jars so JavaParser can resolve every parameter type to its JVM descriptor.
val lines = mapOf(
    "f23" to listOf("2.3.0", "org.apache.flink:flink-runtime:2.3.0:sources", "org.apache.flink:flink-streaming-java:2.3.0"),
    "f120" to listOf("1.20.5", "org.apache.flink:flink-streaming-java:1.20.5:sources", "org.apache.flink:flink-streaming-java:1.20.5"),
)
lines.forEach { (line, spec) ->
    val (version, sources, binary) = spec
    val src = configurations.create("sources_$line") { isTransitive = false }
    val bin = configurations.create("binary_$line")
    dependencies { add(src.name, sources); add(bin.name, binary) }
    val out = rootProject.file("views-$line/flink-api.tsv")
    tasks.register<JavaExec>("extract_$line") {
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("flinkt.extract.ExtractKt")
        inputs.files(src, bin)
        outputs.file(out)
        argumentProviders.add(CommandLineArgumentProvider { listOf(version, src.singleFile.path, out.path) + bin.files.map { it.path } })
    }
}
