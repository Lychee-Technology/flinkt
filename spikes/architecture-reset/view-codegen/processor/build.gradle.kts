plugins { kotlin("jvm") }
kotlin { jvmToolchain(25); compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } }
java { targetCompatibility = JavaVersion.VERSION_17 }
dependencies {
    implementation("com.google.devtools.ksp:symbol-processing-api:2.3.12")
    implementation("com.squareup:kotlinpoet:2.4.0")
    implementation("com.squareup:kotlinpoet-ksp:2.4.0")
}
