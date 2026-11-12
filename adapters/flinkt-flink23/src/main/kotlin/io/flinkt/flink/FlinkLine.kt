package io.flinkt.flink

import org.apache.flink.FlinkVersion

/** The Flink minor line this adapter is built for. `FlinkVersion.v2_3` doesn't exist on Flink 1.20. */
internal val ADAPTER_FLINK_LINE: FlinkVersion = FlinkVersion.v2_3
