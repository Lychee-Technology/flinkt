plugins { kotlin("jvm") }
kotlin { jvmToolchain(25) }
dependencies { implementation("org.ow2.asm:asm-tree:9.10.1") }

// One extraction per Flink line, from the same binary jars the adapter compiles against.
val lines = mapOf(
    "f23" to listOf("2.3.0", "org.apache.flink:flink-streaming-java:2.3.0"),
    "f120" to listOf("1.20.5", "org.apache.flink:flink-streaming-java:1.20.5"),
)
lines.forEach { (line, spec) ->
    val (version, binary) = spec
    val bin = configurations.create("binary_$line")
    dependencies { add(bin.name, binary) }
    val out = rootProject.file("views-$line/flink-api.tsv")
    tasks.register<JavaExec>("extract_$line") {
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("flinkt.extract.ExtractKt")
        inputs.files(bin)
        outputs.file(out)
        argumentProviders.add(CommandLineArgumentProvider { listOf(version, out.path) + bin.files.map { it.path } })
    }
}
