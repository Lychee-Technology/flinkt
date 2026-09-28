// SPIKE (disposable): stand-in for the Flinkt type runtime. Not production code.
package spike.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import java.util.ServiceLoader
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.typeOf

class UnsupportedFlinktType(message: String) : IllegalArgumentException(message)

/** Version-independent SPI that KSP output would implement. It mentions no Flink type. */
interface GeneratedCodec<T> : java.io.Serializable {
    val typeClass: Class<T>
    fun write(value: T, out: java.io.DataOutput)
    fun read(input: java.io.DataInput): T
}

interface GeneratedTypeModule {
    fun register(registry: MutableTypeRegistry)
}

class MutableTypeRegistry internal constructor() {
    internal val codecs = HashMap<Class<*>, GeneratedCodec<*>>()
    fun <T> register(codec: GeneratedCodec<T>) {
        codecs[codec.typeClass] = codec
    }
}

object TypeRegistry {
    private val registry: MutableTypeRegistry by lazy {
        val r = MutableTypeRegistry()
        val cl = Thread.currentThread().contextClassLoader ?: GeneratedTypeModule::class.java.classLoader
        ServiceLoader.load(GeneratedTypeModule::class.java, cl).forEach { it.register(r) }
        r
    }

    fun codecFor(c: Class<*>): GeneratedCodec<*>? = registry.codecs[c]
}

@PublishedApi
internal fun resolve(type: KType): TypeInformation<*> {
    val cls = type.classifier as? KClass<*>
        ?: throw UnsupportedFlinktType("$type is a type parameter; Flinkt needs a concrete type")
    if (type.isMarkedNullable) throw UnsupportedFlinktType("$type is nullable; the spike models only non-null types")
    val args = type.arguments.map { p ->
        p.type?.let(::resolve) ?: throw UnsupportedFlinktType("star projection in $type")
    }
    return when (cls) {
        Long::class -> Types.LONG
        Int::class -> Types.INT
        String::class -> Types.STRING
        Boolean::class -> Types.BOOLEAN
        List::class -> Types.LIST(args[0])
        else -> TypeRegistry.codecFor(cls.java)?.let { GeneratedTypeInfo(it) }
            ?: throw UnsupportedFlinktType(
                "$type is not a Flinkt-modeled type. Annotate it with @FlinkType or pass an explicit " +
                    "TypeInformation. Flinkt does not fall back to Kryo.",
            )
    }
}

inline fun <reified T> typeInfo(): TypeInformation<T> {
    @Suppress("UNCHECKED_CAST")
    return resolve(typeOf<T>()) as TypeInformation<T>
}

/** Test helper: the exact static type the compiler inferred for an expression. */
inline fun <reified T> staticType(@Suppress("UNUSED_PARAMETER") value: T): String = typeOf<T>().toString()
