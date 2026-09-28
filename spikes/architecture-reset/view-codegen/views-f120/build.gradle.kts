plugins {
    kotlin("jvm")
    id("com.google.devtools.ksp")
}
kotlin {
    jvmToolchain(25)
    explicitApi()
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
    sourceSets["main"].kotlin.srcDir("../views/src/main/kotlin")
    sourceSets["main"].kotlin.srcDir("../probe/src/main/kotlin")
    // Negative cases: -PnegCase=<dir under neg/> adds one bad view declaration.
    (findProperty("negCase") as String?)?.let { sourceSets["main"].kotlin.srcDir("../neg/$it") }
}
java { targetCompatibility = JavaVersion.VERSION_17 }
ksp {
    arg("flinkt.flinkVersion", "1.20.5")
    (findProperty("extraForward") as String?)?.let { arg("flinkt.extraForward", it) }
}
dependencies {
    implementation(project(":annotations"))
    ksp(project(":processor"))
    compileOnly("org.apache.flink:flink-streaming-java:1.20.5")
    testImplementation("org.apache.flink:flink-streaming-java:1.20.5")
}
val probeClasspath by configurations.creating
dependencies {
    probeClasspath("org.apache.flink:flink-streaming-java:1.20.5")
    probeClasspath("org.apache.flink:flink-clients:1.20.5")
    probeClasspath("org.slf4j:slf4j-simple:1.7.36")
}
tasks.register<JavaExec>("probe") {
    classpath = sourceSets["main"].runtimeClasspath + probeClasspath
    mainClass.set("flinkt.probe.ProbeKt")
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED", "--add-opens", "java.base/java.util=ALL-UNNAMED", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn")
}
