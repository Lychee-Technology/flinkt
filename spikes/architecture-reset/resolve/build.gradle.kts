plugins { base }
repositories { mavenCentral() }
val lines = mapOf("f120" to "1.20.5", "f22" to "2.2.1", "f23" to "2.3.0")
lines.forEach { (name, v) ->
    val conf = configurations.create(name)
    dependencies {
        add(name, "org.apache.flink:flink-streaming-java:$v")
        add(name, "org.apache.flink:flink-clients:$v")
        add(name, "org.slf4j:slf4j-simple:1.7.36")
    }
    tasks.register<Sync>("copy_$name") {
        from(conf)
        into(layout.projectDirectory.dir("../libs/$name"))
    }
}
tasks.register("copyAll") { dependsOn(lines.keys.map { "copy_$it" }) }
