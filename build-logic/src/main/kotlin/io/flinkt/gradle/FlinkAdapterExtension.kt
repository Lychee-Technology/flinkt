package io.flinkt.gradle

import org.gradle.api.provider.Property

/** The `flinkAdapter { }` block of a Flink adapter module. */
public abstract class FlinkAdapterExtension {
    /** The Flink release the adapter compiles against and its tests run on, from `gradle/libs.versions.toml`. */
    public abstract val flinkVersion: Property<String>
}
