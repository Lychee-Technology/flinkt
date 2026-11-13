package io.flinkt.flink

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

// Entering a view. Kotlin picks the extension with the most specific receiver from the stream's static type, so a
// `DataStreamSource` enters as a `FlinktSingleOutputStreamOperator`, and a `KeyedStream` as a `FlinktKeyedStream`.
// Each creates the view and does nothing else: it reads nothing from the stream, its type included, because Flink
// marks a stream's type as used when it's read, and a later `returns()` on the stream would then throw.

/** A view of this stream, which holds this instance and nothing else. Its `asFlink()` returns this instance. */
public fun <T> DataStream<T>.flinkt(): FlinktDataStream<T> = FlinktDataStream(this)

/**
 * A view of this operator or source, which holds this instance and nothing else. Its `asFlink()` returns this
 * instance.
 */
public fun <T> SingleOutputStreamOperator<T>.flinkt(): FlinktSingleOutputStreamOperator<T> =
    FlinktSingleOutputStreamOperator(this)

/** A view of this keyed stream, which holds this instance and nothing else. Its `asFlink()` returns this instance. */
public fun <T, K> KeyedStream<T, K>.flinkt(): FlinktKeyedStream<T, K> = FlinktKeyedStream(this)
