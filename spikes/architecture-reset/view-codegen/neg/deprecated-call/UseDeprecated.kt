// SPIKE: a Kotlin caller of a forwarded method that Flink 1.20 deprecated.
package flinkt.probe.deprecated

import flinkt.views.FlinktDataStream
import org.apache.flink.api.common.functions.Partitioner

internal fun useDeprecated(view: FlinktDataStream<Long>) =
    view.partitionCustom(Partitioner<Long> { key, n -> (key % n).toInt() }, 0)
