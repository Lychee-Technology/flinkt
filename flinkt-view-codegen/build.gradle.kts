// Runs only in Flinkt's own build, as the KSP processor that generates each adapter's forwarders.
// It isn't published, so it has no ABI dump.
plugins {
    id("flinkt.kotlin-jvm")
}

dependencies {
    implementation(libs.ksp.api)
}
