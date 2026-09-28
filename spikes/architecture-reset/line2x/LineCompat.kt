// SPIKE: Flink 2.x — createSerializer(SerializerConfig) is the only abstract factory.
package spike.types

import org.apache.flink.api.common.typeinfo.TypeInformation

abstract class LineTypeInfo<T> : TypeInformation<T>()
