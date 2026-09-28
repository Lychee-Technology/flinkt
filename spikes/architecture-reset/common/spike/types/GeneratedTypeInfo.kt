// SPIKE (disposable): adapter-side Flink protocol code shared by every generated type.
package spike.types

import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.core.memory.DataOutputView

class GeneratedTypeInfo<T>(val codec: GeneratedCodec<T>) : LineTypeInfo<T>() {
    override fun isBasicType() = false
    override fun isTupleType() = false
    override fun getArity() = 1
    override fun getTotalFields() = 1
    override fun getTypeClass(): Class<T> = codec.typeClass
    override fun isKeyType() = false
    override fun createSerializer(config: SerializerConfig): TypeSerializer<T> = GeneratedSerializer(codec)
    override fun toString() = "GeneratedTypeInfo<${codec.typeClass.simpleName}>"
    override fun equals(other: Any?) = other is GeneratedTypeInfo<*> && other.codec.typeClass == codec.typeClass
    override fun hashCode() = codec.typeClass.hashCode()
    override fun canEqual(obj: Any?) = obj is GeneratedTypeInfo<*>
}

class GeneratedSerializer<T>(val codec: GeneratedCodec<T>) : TypeSerializer<T>() {
    override fun isImmutableType() = true
    override fun duplicate() = this
    override fun createInstance(): T? = null
    override fun copy(from: T): T = from
    override fun copy(from: T, reuse: T): T = from
    override fun getLength() = -1
    override fun serialize(record: T, target: DataOutputView) = codec.write(record, target)
    override fun deserialize(source: DataInputView): T = codec.read(source)
    override fun deserialize(reuse: T, source: DataInputView): T = codec.read(source)
    override fun copy(source: DataInputView, target: DataOutputView) = serialize(deserialize(source), target)
    override fun equals(other: Any?) = other is GeneratedSerializer<*> && other.codec.typeClass == codec.typeClass
    override fun hashCode() = codec.typeClass.hashCode()
    override fun snapshotConfiguration(): TypeSerializerSnapshot<T> = GeneratedSnapshot(codec.typeClass)
}

class GeneratedSnapshot<T>() : TypeSerializerSnapshot<T> {
    private var typeClass: Class<T>? = null

    constructor(typeClass: Class<T>) : this() {
        this.typeClass = typeClass
    }

    override fun getCurrentVersion() = 1
    override fun writeSnapshot(out: DataOutputView) = out.writeUTF(typeClass!!.name)

    @Suppress("UNCHECKED_CAST")
    override fun readSnapshot(readVersion: Int, input: DataInputView, userCodeClassLoader: ClassLoader) {
        typeClass = Class.forName(input.readUTF(), false, userCodeClassLoader) as Class<T>
    }

    @Suppress("UNCHECKED_CAST")
    override fun restoreSerializer(): TypeSerializer<T> =
        GeneratedSerializer(TypeRegistry.codecFor(typeClass!!) as GeneratedCodec<T>? ?: error("no codec for $typeClass"))

    override fun resolveSchemaCompatibility(oldSerializerSnapshot: TypeSerializerSnapshot<T>): TypeSerializerSchemaCompatibility<T> =
        if (oldSerializerSnapshot is GeneratedSnapshot<T> && oldSerializerSnapshot.typeClass == typeClass) {
            TypeSerializerSchemaCompatibility.compatibleAsIs()
        } else {
            TypeSerializerSchemaCompatibility.incompatible()
        }
}
