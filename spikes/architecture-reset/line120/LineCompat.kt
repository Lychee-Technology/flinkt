// SPIKE: Flink 1.20 — createSerializer(ExecutionConfig) is still abstract (deprecated).
package spike.types

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

abstract class LineTypeInfo<T> : TypeInformation<T>() {
    @Deprecated("Flink 1.20 abstract method")
    override fun createSerializer(config: ExecutionConfig): TypeSerializer<T> = createSerializer(config.serializerConfig)
}
