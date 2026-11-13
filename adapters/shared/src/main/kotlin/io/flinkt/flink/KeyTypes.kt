package io.flinkt.flink

import io.flinkt.registry.TypeRegistry
import io.flinkt.types.BuiltInKind
import io.flinkt.types.TypeShape
import java.util.EnumSet
import kotlin.reflect.KType
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

// The interim key policy of the view's keyBy, until Flinkt's key rules (docs/architecture.md, "Keys") are
// implemented in #32.
//
// Flink's keyBy doesn't read isKeyType(), and KeyedStream.validateKeyType rejects only POJOs without hashCode(),
// arrays and EnumTypeInfo, so Flink would accept any TypeInformation that Flinkt later builds, one for a key whose
// hashCode() differs between JVMs included. Until #32, keyBy therefore accepts only the key types whose hash codes and
// bytes are specified: the non-null built-ins that typeInfo() resolves to Flink's own BasicTypeInfo (#7). It refuses
// every other key type before it calls Flink, whether or not typeInfo() has a TypeInformation for it yet.

/**
 * The built-in kinds keyBy accepts as a key. Listed, not taken from `BuiltInKind.entries`, so that a kind core adds,
 * and a mapping typeInfo() adds for it, don't make it a key by themselves.
 */
private val KEY_KINDS: Set<BuiltInKind> = EnumSet.of(
    BuiltInKind.BOOLEAN,
    BuiltInKind.BYTE,
    BuiltInKind.SHORT,
    BuiltInKind.INT,
    BuiltInKind.LONG,
    BuiltInKind.FLOAT,
    BuiltInKind.DOUBLE,
    BuiltInKind.CHAR,
    BuiltInKind.STRING,
)

/**
 * Flink's `TypeInformation` for each of [KEY_KINDS], the constant that typeInfo() gives for it. [checkKeyType] compares
 * by equality, so a copy of a constant, such as the one Java serialization gives back, is accepted too.
 */
private val KEY_TYPES: List<TypeInformation<*>> = listOf(
    BasicTypeInfo.BOOLEAN_TYPE_INFO,
    BasicTypeInfo.BYTE_TYPE_INFO,
    BasicTypeInfo.SHORT_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.FLOAT_TYPE_INFO,
    BasicTypeInfo.DOUBLE_TYPE_INFO,
    BasicTypeInfo.CHAR_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
)

/**
 * The `TypeInformation` the reified keyBy passes Flink for its key type [type]: typeInfo()'s, once [type] passed the
 * key policy. [type] is checked first, so a key type that typeInfo() resolves in a later release is still refused.
 * Its JVM signature is part of the adapter's ABI, because the inline keyBy calls it.
 */
@PublishedApi
internal fun keyTypeInfo(type: KType): TypeInformation<*> {
    // A built-in's model is the same in every registry, so an empty one decides it, and no module is loaded for it.
    val model = TypeRegistry.of().model(type)
    val shape = model.shape
    if (model.isNullable || shape !is TypeShape.BuiltIn || shape.kind !in KEY_KINDS) {
        throw IllegalArgumentException(
            "keyBy doesn't accept $model as a key type yet. Until Flinkt's key rules are implemented, the view's keyBy " +
                "accepts only a non-null $KEY_NAMES key, and keys of other types wait for those rules. $ESCAPE",
        )
    }
    return typeInfo(type)
}

/**
 * Refuses a [keyType] that isn't Flink's `BasicTypeInfo` for one of [KEY_KINDS].
 *
 * It accepts a `TypeInformation` equal to one of [KEY_TYPES], not only the constant itself: a `TypeInformation` is
 * `Serializable`, and one that a job or a library deserialized is a copy, equal to the constant but not the same
 * object. `BasicTypeInfo.equals` compares the boxed class, the comparator class and the serializer, and Flink's
 * built-in serializers are equal only to a serializer of their own class, so an equal copy gives a key the same bytes
 * and comparisons. Its class has to be the constant's own class, `BasicTypeInfo`, `IntegerTypeInfo` or
 * `FractionalTypeInfo`: a subclass can override `createSerializer` and still be equal, because
 * `BasicTypeInfo.canEqual` accepts every subclass.
 *
 * A `TypeInformation` doesn't carry the Kotlin type, so the message names it by its `toString()` and its class.
 */
internal fun checkKeyType(keyType: TypeInformation<*>) {
    if (KEY_TYPES.none { it.javaClass == keyType.javaClass && it == keyType }) {
        throw IllegalArgumentException(
            "keyBy doesn't accept the key type $keyType (${keyType.javaClass.name}) yet. Until Flinkt's key rules " +
                "are implemented, the view's keyBy accepts only Flink's BasicTypeInfo for a non-null $KEY_NAMES key, " +
                "the one typeInfo() gives for it, and keys of other types wait for those rules. $ESCAPE",
        )
    }
}

private const val KEY_NAMES = "Boolean, Byte, Short, Int, Long, Float, Double, Char or String"

private const val ESCAPE = "To key the stream by Flink's own rules instead, call asFlink().keyBy(...)."
