// SPIKE (disposable): descriptor construction and runtime binding as separately named helpers.
package spike.state

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import spike.types.typeInfo

inline fun <reified T> valueStateDescriptor(name: String): ValueStateDescriptor<T> =
    ValueStateDescriptor(name, typeInfo<T>())

inline fun <reified T> listStateDescriptor(name: String): ListStateDescriptor<T> =
    ListStateDescriptor(name, typeInfo<T>())

inline fun <reified K, reified V> mapStateDescriptor(name: String): MapStateDescriptor<K, V> =
    MapStateDescriptor(name, typeInfo<K>(), typeInfo<V>())

inline fun <reified T> RuntimeContext.valueState(name: String): ValueState<T> =
    getState(valueStateDescriptor<T>(name))

inline fun <reified T> RuntimeContext.listState(name: String): ListState<T> =
    getListState(listStateDescriptor<T>(name))

inline fun <reified K, reified V> RuntimeContext.mapState(name: String): MapState<K, V> =
    getMapState(mapStateDescriptor<K, V>(name))
