package io.flinkt.flink

import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.types.BuiltInKind
import io.flinkt.types.UnsupportedTypeException
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * Flink's `TypeInformation` for the full Kotlin type [T].
 *
 * [T] is read with `typeOf<T>()`, which keeps its classifier, every type argument and nullability at every level, and
 * needs no `kotlin-reflect`. `T::class.java` would lose the arguments and the `?`.
 *
 * This release resolves the built-in types. A non-null `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`,
 * `Char` or `String` gives Flink's own [BasicTypeInfo] for its JVM class, the boxed class for a primitive, so Flink
 * creates its own serializer for it, such as `IntSerializer`, and the bytes are Flink's. A platform type from Java,
 * such as the `String!` that a Java method returns, resolves as non-null.
 *
 * Every other type fails here, nullable types, `List`, `Map` and generated types included. The
 * [UnsupportedTypeException] names the full Kotlin type, such as
 * `kotlin.collections.Map<com.example.UserId, kotlin.collections.List<com.example.Event?>>`, says why, and gives the
 * fixes: annotate the type with `@FlinkType`, or pass a `TypeInformation` explicitly. A type Flinkt can't resolve
 * never falls back to Flink's `TypeExtractor` or to a `GenericTypeInfo`, so it never becomes Kryo silently.
 *
 * @throws UnsupportedTypeException if Flinkt has no `TypeInformation` for [T].
 */
public inline fun <reified T> typeInfo(): TypeInformation<T> {
    @Suppress("UNCHECKED_CAST")
    return typeInfo(typeOf<T>()) as TypeInformation<T>
}

/**
 * [typeInfo] for a `KType`, which the inline function passes from its call site. Its JVM signature is part of the
 * adapter's ABI, because the inline function's callers call it.
 */
@PublishedApi
internal fun typeInfo(type: KType): TypeInformation<*> {
    val resolution = try {
        registry().resolve(type)
    } catch (e: UnsupportedTypeException) {
        throw UnsupportedTypeException(e.canonicalType, "${e.reason}. $FIXES", e)
    }
    return when (resolution) {
        is Resolution.BuiltIn -> builtIn(resolution.kind)
        is Resolution.Codec -> throw UnsupportedTypeException(
            resolution.type.toString(),
            "it's a generated type, and typeInfo() has no TypeInformation for a generated type yet. Pass a " +
                "TypeInformation explicitly",
        )
    }
}

private const val FIXES = "To give Flink a TypeInformation for it, annotate the type with @FlinkType, or pass a " +
    "TypeInformation explicitly"

/**
 * Flink's own `TypeInformation` for a built-in, the one Flink uses for its JVM class.
 *
 * The `when` has no fallback branch, so a kind that `flinkt-core` adds fails to compile here until it's mapped. Each
 * mapping fixes the state format of the type, so each new one is its own change, with its reason recorded.
 */
private fun builtIn(kind: BuiltInKind): TypeInformation<*> = when (kind) {
    BuiltInKind.BOOLEAN -> BasicTypeInfo.BOOLEAN_TYPE_INFO
    BuiltInKind.BYTE -> BasicTypeInfo.BYTE_TYPE_INFO
    BuiltInKind.SHORT -> BasicTypeInfo.SHORT_TYPE_INFO
    BuiltInKind.INT -> BasicTypeInfo.INT_TYPE_INFO
    BuiltInKind.LONG -> BasicTypeInfo.LONG_TYPE_INFO
    BuiltInKind.FLOAT -> BasicTypeInfo.FLOAT_TYPE_INFO
    BuiltInKind.DOUBLE -> BasicTypeInfo.DOUBLE_TYPE_INFO
    BuiltInKind.CHAR -> BasicTypeInfo.CHAR_TYPE_INFO
    BuiltInKind.STRING -> BasicTypeInfo.STRING_TYPE_INFO
}

/**
 * The registry [typeInfo] resolves with: a new one on each call, built from the generated modules that the current
 * thread's context class loader finds. In a task, Flink sets that loader to the job's user-code class loader before it
 * runs user code.
 *
 * A built-in resolves to its kind in every registry, so today the choice only decides how a generated type fails. Which
 * class loader a generated type is resolved with, and how long a registry lives, are decided with the `TypeInformation`
 * for generated types (#12) and the registry lifecycle (#24).
 */
private fun registry(): TypeRegistry =
    TypeRegistry.load(Thread.currentThread().contextClassLoader ?: TypeRegistry::class.java.classLoader)
