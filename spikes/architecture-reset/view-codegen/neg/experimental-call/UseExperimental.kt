// SPIKE: a Kotlin caller of a forwarded Flink @Experimental method (2.x), without and with opt-in.
package flinkt.probe.experimental

import flinkt.views.ExperimentalFlinkApi
import flinkt.views.FlinktSingleOutputStreamOperator

internal fun withoutOptIn(op: FlinktSingleOutputStreamOperator<Long>) = op.enableAsyncState()

@OptIn(ExperimentalFlinkApi::class)
internal fun withOptIn(op: FlinktSingleOutputStreamOperator<Long>) = op.enableAsyncState()
