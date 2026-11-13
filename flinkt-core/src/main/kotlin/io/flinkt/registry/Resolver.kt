package io.flinkt.registry

import io.flinkt.codec.CodecContext
import io.flinkt.codec.CodecFactory
import io.flinkt.codec.GeneratedCodec
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.ValueClassSchema
import io.flinkt.types.CollectionKind
import io.flinkt.types.FlinktType
import io.flinkt.types.TypeShape
import io.flinkt.types.UnsupportedTypeException
import kotlin.reflect.KType

/**
 * Supported resolution of one type and of everything its codec needs, which the codec factories resolve through
 * [CodecContext.codec] while they run.
 *
 * Each shape that a later release supports changes a branch here, not the model: nullable types, collections, value
 * classes, enums, sealed hierarchies and generic classes are all modeled already and fail here until then.
 */
internal class Resolver(private val registry: TypeRegistry) {
    /** The types whose codecs are being created, outermost first, to stop a type that contains itself. */
    private val creating = ArrayList<FlinktType>()

    fun resolve(type: FlinktType): Resolution {
        if (type.isNullable) unsupported(type, "nullable types aren't supported yet")
        return when (val shape = type.shape) {
            // A built-in's bytes are Flink's: the adapter gives Flink its own TypeInformation for the kind.
            is TypeShape.BuiltIn -> Resolution.BuiltIn(type, shape.kind)
            is TypeShape.Collection -> unsupported(type, collectionReason(shape.kind))
            is TypeShape.Generated -> Resolution.Codec(type, generated(type))
            TypeShape.ClassifierOnly -> unsupported(type, registry.unmodeledReason(type))
            TypeShape.TypeParameter -> unsupported(
                type,
                "it's a type parameter, so its type isn't known here. Resolve the type where its arguments are " +
                    "known, such as in an inline function with a reified type parameter",
            )
        }
    }

    /**
     * A generated declaration. The schema that decides support and the factory are this registry's registration of
     * the class, the same one, so a schema from anywhere else can't select a factory.
     */
    private fun generated(type: FlinktType): GeneratedCodec<*> {
        val declaration = registry.declaration(checkNotNull(type.jvmClass))
            ?: unsupported(type, registry.unmodeledReason(type))
        when (val schema = declaration.schema) {
            is RecordSchema -> if (schema.typeParameters.isNotEmpty()) unsupported(type, "generic classes aren't supported yet")
            is ValueClassSchema -> unsupported(type, "value classes aren't supported yet")
            is EnumSchema -> unsupported(type, "enums aren't supported yet")
            is SealedSchema -> unsupported(type, "sealed types aren't supported yet")
            is ObjectSchema -> unsupported(type, "objects aren't supported yet")
        }
        return create(type, declaration.codecFactory)
    }

    private fun create(type: FlinktType, factory: CodecFactory<*>): GeneratedCodec<*> {
        if (type in creating) {
            val cycle = (creating.subList(creating.indexOf(type), creating.size) + type).joinToString(" -> ")
            unsupported(type, "its codec needs itself ($cycle), and recursive types aren't supported yet")
        }
        creating += type
        val context = Context(type)
        try {
            return factory.create(context)
        } finally {
            context.open = false
            creating.removeAt(creating.lastIndex)
        }
    }

    private inner class Context(override val type: FlinktType) : CodecContext {
        var open = true

        // Generic classes aren't resolved yet, so no codec has arguments.
        override val argumentCodecs: List<GeneratedCodec<*>> get() = emptyList()

        override fun codec(type: KType): GeneratedCodec<*> {
            check(open) {
                "The CodecContext of ${this.type} was used after its codec was created. A codec gets the codecs it " +
                    "needs while it's created, and keeps them"
            }
            val resolution = try {
                resolve(registry.model(type))
            } catch (e: UnsupportedTypeException) {
                throw UnsupportedTypeException(
                    this.type.toString(),
                    "it contains ${e.canonicalType}, which Flinkt can't resolve: ${e.reason}",
                    e,
                )
            }
            return when (resolution) {
                is Resolution.Codec -> resolution.codec
                // A built-in's codec is needed only where one is composed, as a List element or a generic argument,
                // and those come with the support for collections and generic classes, in #5's scalar format.
                is Resolution.BuiltIn -> unsupported(
                    this.type,
                    "its codec asks for a codec of ${resolution.type}, a built-in type, and Flinkt has no codec for a " +
                        "built-in type yet. Generated code writes a field of a built-in type itself",
                )
            }
        }
    }
}

private fun collectionReason(kind: CollectionKind): String = when (kind) {
    CollectionKind.LIST, CollectionKind.MAP -> "List and Map aren't supported yet"
    else -> "of the collection types, Flinkt models only the read-only List and Map"
}

private fun unsupported(type: FlinktType, reason: String): Nothing =
    throw UnsupportedTypeException(type.toString(), reason)
