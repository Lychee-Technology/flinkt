package io.flinkt.flink

import org.apache.flink.FlinkVersion

/**
 * Whether [flink] is the Flink minor line this adapter is built for, [ADAPTER_FLINK_LINE].
 *
 * The default is the line of the Flink classes on the classpath. [FlinkVersion.current] returns the newest constant
 * of the loaded `FlinkVersion` enum, which is `@Public` on every target line.
 */
internal fun isAdapterLine(flink: FlinkVersion = FlinkVersion.current()): Boolean = flink == ADAPTER_FLINK_LINE
