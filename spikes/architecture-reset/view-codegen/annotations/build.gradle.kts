plugins { kotlin("jvm") }
kotlin { jvmToolchain(25); compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } }
java { targetCompatibility = JavaVersion.VERSION_17 }
