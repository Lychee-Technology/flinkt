package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.ValueClassSchema
import io.flinkt.types.UnsupportedTypeException
import java.io.DataInput
import java.io.IOException
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.core.memory.DataOutputView

/*
 * A prototype of the restore design (docs/persisted-format.md, "Restore"), evidence for #5. #12 implements the
 * serializer and the snapshot class, #34 the planner, #35 the old-layout serializer, and #36 to #38 enable the
 * migrations. Generated codecs here are hand-written stand-ins compiled with the declarations they serve, as KSP's
 * will be (#11).
 */

/**
 * What a generated codec offers a migration: the current primary constructor, called directly with resolved values
 * (docs/schema-evolution.md, "Constructing current values"). The prototype's stand-in for KSP's migration constructor.
 */
interface MigrationConstructor<T> {
    /** The current primary constructor's parameters, in constructor order. */
    val parameterNames: List<String>

    /** Calls the primary constructor with [arguments], in constructor order. */
    fun construct(arguments: Array<Any?>): T
}

/** A migration operation, and the stage that enables it (docs/schema-evolution.md, "Stages"). */
enum class Operation(val stage: String) {
    REMOVE_FIELD("stage 4, #36"),
}

/** Restoring this state would read its bytes wrongly, so it fails instead. */
class IncompatibleStateException(message: String) : IllegalStateException(message)

/** The planner's answer for an old snapshot and the current declaration (docs/schema-evolution.md, "The planner"). */
sealed class Plan {
    object AsIs : Plan()

    /** Read with the old-layout serializer: each old field of the root record is kept or removed. */
    class Migrate(val operations: Map<String, FieldOperation>) : Plan()

    class Incompatible(val message: String) : Plan()
}

enum class FieldOperation { KEEP, REMOVE }

/**
 * The prototype planner. It covers what the restore prototypes need: an identical schema, a constructor reorder in
 * each role, fields removed from or added to the root record, and type changes. #34 plans every shape.
 */
object Planner {
    fun plan(old: PersistedSchema, current: PersistedSchema, currentRole: KeyRole, enabled: Set<Operation>): Plan {
        val type = old.root.toString()
        val roles = listOf(old.role, currentRole).distinct()
        val role = roles.joinToString(" and ") { it.name.lowercase().replace('_', ' ') }
        fun incompatible(detail: String) = Plan.Incompatible("Can't restore $type (role: $role): $detail")

        if (old.recordFormatVersion != RecordFormat.VERSION) {
            return incompatible(
                "its records were written in record format version ${old.recordFormatVersion}, and this release " +
                    "reads version ${RecordFormat.VERSION}",
            )
        }
        val partitionKey = KeyRole.PARTITION_KEY in roles
        if (old.sameLayout(current, compareConstructorOrder = partitionKey)) return Plan.AsIs
        if (partitionKey && old.sameLayout(current, compareConstructorOrder = false)) {
            val reordered = old.declarations.values.filterIsInstance<RecordSchema>().first { record ->
                record.fields.map { it.name } != (current.declarations.getValue(record.typeName) as RecordSchema).fields.map { it.name }
            }
            val now = current.declarations.getValue(reordered.typeName) as RecordSchema
            return incompatible(
                "the constructor order of ${reordered.typeName} changed from (${reordered.fields.joinToString { it.name }}) " +
                    "to (${now.fields.joinToString { it.name }}). Its bytes didn't change, but a data class's generated " +
                    "hashCode() follows constructor order, so the partition key's key groups would move",
            )
        }
        val firstDifference = difference(old, current)
        if (roles.any { it != KeyRole.VALUE }) {
            return incompatible("a key's schema doesn't change, and $firstDifference")
        }
        if (old.root.name != current.root.name) return incompatible("the class changed: $firstDifference")
        val oldRecord = old.declarations[old.root.name] as? RecordSchema ?: return incompatible(firstDifference)
        val currentRecord = current.declarations[current.root.name] as? RecordSchema ?: return incompatible(firstDifference)

        val currentFields = currentRecord.fields.associateBy { it.name }
        val added = currentRecord.fields.firstOrNull { field -> oldRecord.fields.none { it.name == field.name } }
        if (added != null) {
            return incompatible(
                "${oldRecord.typeName}.${added.name} was added as ${added.type}, without a migration default (#37). " +
                    "Old: $oldRecord. Current: $currentRecord",
            )
        }
        val operations = LinkedHashMap<String, FieldOperation>()
        for (field in oldRecord.fields) {
            val now = currentFields[field.name]
            operations[field.name] = when {
                now == null -> FieldOperation.REMOVE
                now.type == field.type && reachable(old, field.type) == reachable(current, now.type) -> FieldOperation.KEEP
                else -> return incompatible(
                    "${oldRecord.typeName}.${field.name} changed from ${field.type} to ${now.type}, or a type it " +
                        "contains changed. Old: $oldRecord. Current: $currentRecord",
                )
            }
        }
        val removed = operations.filterValues { it == FieldOperation.REMOVE }.keys
        if (removed.isNotEmpty() && Operation.REMOVE_FIELD !in enabled) {
            return incompatible(
                "${removed.joinToString { "${oldRecord.typeName}.$it" }} was removed. Removing a field migrates from " +
                    "${Operation.REMOVE_FIELD.stage}, and is incompatible until then. Old: $oldRecord. Current: $currentRecord",
            )
        }
        return Plan.Migrate(operations)
    }

    private fun difference(old: PersistedSchema, current: PersistedSchema): String {
        if (old.root != current.root) return "the serialized type changed from ${old.root} to ${current.root}"
        val names = (old.declarations.keys + current.declarations.keys).sorted()
        val name = names.first { old.declarations[it] != current.declarations[it] }
        return "the declaration of $name changed. Old: ${old.declarations[name] ?: "none"}. Current: ${current.declarations[name] ?: "none"}"
    }

    private fun reachable(schema: PersistedSchema, type: SchemaType): Map<String, DeclarationSchema> {
        val found = sortedMapOf<String, DeclarationSchema>()
        fun visit(t: SchemaType?) {
            if (t !is SchemaType.ClassType) return
            schema.declarations[t.name]?.let { declaration ->
                if (found.put(t.name, declaration) == null) PersistedSchema.memberTypes(declaration).forEach(::visit)
            }
            t.arguments.forEach { visit(it.type) }
        }
        visit(type)
        return found
    }
}

/** The current declaration of a restored state's type, found through the user-code class loader Flink passes. */
class CurrentType<T>(
    val codec: GeneratedCodec<T>,
    val schema: PersistedSchema,
    val registry: TypeRegistry,
    val classLoader: ClassLoader,
) {
    companion object {
        fun <T> find(root: SchemaType.ClassType, role: KeyRole, classLoader: ClassLoader): CurrentType<T> {
            val jvmClass = try {
                Class.forName(root.name, false, classLoader)
            } catch (e: ClassNotFoundException) {
                throw IncompatibleStateException(
                    "Can't restore $root: the job has no class ${root.name}, so nothing can construct the restored " +
                        "values. A state whose type is gone fails the restore, even if the job no longer uses it",
                )
            }
            val registry = TypeRegistry.load(classLoader)
            val codec = try {
                registry.resolve(kotlin.jvm.internal.Reflection.typeOf(jvmClass))
            } catch (e: UnsupportedTypeException) {
                throw IncompatibleStateException("Can't restore $root: the current job has no codec for it. ${e.message}")
            }
            val schema = try {
                PersistedSchema.of(root, role) { name ->
                    try {
                        registry.declaration(Class.forName(name, false, classLoader))?.schema
                    } catch (e: ClassNotFoundException) {
                        null
                    }
                }
            } catch (e: IllegalArgumentException) {
                throw IncompatibleStateException("Can't restore $root: ${e.message}")
            }
            @Suppress("UNCHECKED_CAST")
            return CurrentType((codec as Resolution.Codec).codec as GeneratedCodec<T>, schema, registry, classLoader)
        }
    }
}

/** The prototype of the one `TypeSerializer` an adapter shares between generated types: its codec and its schema. */
class PrototypeSerializer<T>(private val codec: GeneratedCodec<T>, val schema: PersistedSchema) : TypeSerializer<T>() {
    override fun isImmutableType(): Boolean = false

    override fun duplicate(): TypeSerializer<T> = this

    override fun createInstance(): T = throw UnsupportedOperationException("A record has no empty instance")

    override fun copy(from: T): T = codec.copy(from)

    override fun copy(from: T, reuse: T): T = codec.copy(from)

    override fun getLength(): Int = -1

    override fun serialize(record: T, target: DataOutputView) = codec.serialize(record, target)

    override fun deserialize(source: DataInputView): T = codec.deserialize(source)

    override fun deserialize(reuse: T, source: DataInputView): T = codec.deserialize(source)

    override fun copy(source: DataInputView, target: DataOutputView) = codec.serialize(codec.deserialize(source), target)

    override fun equals(other: Any?): Boolean =
        other is PrototypeSerializer<*> && other.codec === codec && other.schema.encoded.contentEquals(schema.encoded)

    override fun hashCode(): Int = schema.root.hashCode()

    override fun snapshotConfiguration(): TypeSerializerSnapshot<T> = PrototypeSerializerSnapshot(schema)
}

/**
 * The prototype of Flinkt's one snapshot class. Flink writes this class's name into the checkpoint and creates it by
 * name, with this public no-argument constructor, through the user-code class loader. The production class is
 * `io.flinkt.flink.FlinktSerializerSnapshot` on every adapter (docs/persisted-format.md, "The snapshot class").
 */
class PrototypeSerializerSnapshot<T> : TypeSerializerSnapshot<T> {
    private var schema: PersistedSchema? = null
    private var userCodeClassLoader: ClassLoader? = null

    /** The constructor Flink calls on restore, before [readSnapshot]. */
    constructor()

    constructor(schema: PersistedSchema) {
        this.schema = schema
    }

    val persistedSchema: PersistedSchema get() = checkNotNull(schema) { "The snapshot hasn't been read" }

    override fun getCurrentVersion(): Int = PersistedSchema.SNAPSHOT_FORMAT_VERSION

    override fun writeSnapshot(out: DataOutputView) {
        persistedSchema.write(out)
        // The adapter's part: the snapshots of Flink serializers this one delegates to, which in format version 1 is
        // only the inner serializer of a top-level nullable built-in (#15). A record root delegates to none.
        out.writeInt(0)
    }

    override fun readSnapshot(readVersion: Int, `in`: DataInputView, userCodeClassLoader: ClassLoader) {
        if (readVersion != PersistedSchema.SNAPSHOT_FORMAT_VERSION) {
            throw IOException(
                "This snapshot was written in Flinkt snapshot format version $readVersion, and this release reads " +
                    "version ${PersistedSchema.SNAPSHOT_FORMAT_VERSION}. Restore it with a Flinkt release that reads it",
            )
        }
        schema = PersistedSchema.read(`in`)
        val nested = `in`.readInt()
        if (nested != 0) throw MalformedDataException("a record's snapshot nests $nested Flink snapshots, and it has none")
        this.userCodeClassLoader = userCodeClassLoader
    }

    /** What Flink asks for at restore. The interim policy (#12): as-is or fail, before #35 adds migration. */
    override fun restoreSerializer(): TypeSerializer<T> = restore(enabled = emptySet())

    /** The restore with the migration operations of [enabled] switched on, as later stages will. */
    fun restore(enabled: Set<Operation>): TypeSerializer<T> {
        val old = persistedSchema
        val classLoader = checkNotNull(userCodeClassLoader) { "restore() needs a snapshot that Flink read" }
        val current = CurrentType.find<T>(old.root, old.role, classLoader)
        return when (val plan = Planner.plan(old, current.schema, old.role, enabled)) {
            Plan.AsIs -> PrototypeSerializer(current.codec, old)
            is Plan.Migrate -> OldLayoutSerializer(old, plan, current)
            is Plan.Incompatible -> throw IncompatibleStateException(plan.message)
        }
    }

    override fun resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot<T>,
    ): TypeSerializerSchemaCompatibility<T> {
        val old = oldSerializerSnapshot as? PrototypeSerializerSnapshot<T>
            ?: return TypeSerializerSchemaCompatibility.incompatible()
        return when (Planner.plan(old.persistedSchema, persistedSchema, persistedSchema.role, enabled = emptySet())) {
            Plan.AsIs -> TypeSerializerSchemaCompatibility.compatibleAsIs()
            is Plan.Migrate -> TypeSerializerSchemaCompatibility.compatibleAfterMigration()
            is Plan.Incompatible -> TypeSerializerSchemaCompatibility.incompatible()
        }
    }
}

/**
 * The prototype of the old-layout serializer (#35): it reads the bytes an old declaration wrote into current values,
 * driven by the old schema from the snapshot and the plan. A removed value is consumed by walking its old schema, so
 * its class needn't exist. It reports the old schema as its snapshot, exactly.
 */
class OldLayoutSerializer<T>(
    private val old: PersistedSchema,
    private val plan: Plan.Migrate,
    private val current: CurrentType<T>,
) : TypeSerializer<T>() {
    private val oldRecord = old.declaration(old.root.name) as RecordSchema
    private val canonicalFields = oldRecord.fields.sortedBy { it.name }
    private val nullableFields = canonicalFields.count { it.type.isNullable }

    @Suppress("UNCHECKED_CAST")
    private val constructor = current.codec as? MigrationConstructor<T>
        ?: throw IncompatibleStateException("${old.root}'s codec has no migration constructor")
    private val walker = SchemaWalker(old)
    private val keptReaders: Map<String, (DataInput) -> Any?> = canonicalFields
        .filter { plan.operations[it.name] == FieldOperation.KEEP }
        .associate { it.name to reader(it.type as SchemaType.ClassType) }

    override fun deserialize(source: DataInputView): T {
        val nulls = RecordFormat.readNullBitmap(source, nullableFields)
        val arguments = arrayOfNulls<Any?>(constructor.parameterNames.size)
        var nullable = 0
        for (field in canonicalFields) {
            val present = !field.type.isNullable || !nulls[nullable++]
            when (plan.operations.getValue(field.name)) {
                FieldOperation.KEEP ->
                    arguments[constructor.parameterNames.indexOf(field.name)] =
                        if (present) keptReaders.getValue(field.name)(source) else null
                FieldOperation.REMOVE ->
                    if (present) walker.skipField(field.type, emptyMap(), source, "${oldRecord.typeName}.${field.name}")
            }
        }
        return constructor.construct(arguments)
    }

    override fun deserialize(reuse: T, source: DataInputView): T = deserialize(source)

    /** What a carried state's removed fields are written as is #35's decision, so the prototype doesn't write. */
    override fun serialize(record: T, target: DataOutputView) =
        throw UnsupportedOperationException("#35 decides what the old layout writes for a removed field")

    override fun snapshotConfiguration(): TypeSerializerSnapshot<T> = PrototypeSerializerSnapshot(old)

    override fun isImmutableType(): Boolean = false

    override fun duplicate(): TypeSerializer<T> = this

    override fun createInstance(): T = throw UnsupportedOperationException()

    override fun copy(from: T): T = current.codec.copy(from)

    override fun copy(from: T, reuse: T): T = current.codec.copy(from)

    override fun getLength(): Int = -1

    override fun copy(source: DataInputView, target: DataOutputView) = throw UnsupportedOperationException()

    override fun equals(other: Any?): Boolean = this === other

    override fun hashCode(): Int = System.identityHashCode(this)

    /** A kept field is unchanged, so a built-in is read as v1 writes it, and a record with its current codec. */
    private fun reader(type: SchemaType.ClassType): (DataInput) -> Any? = when (type.name) {
        in PersistedSchema.BUILT_INS -> { input -> readBuiltIn(type.name, input) }
        else -> {
            val codec = (current.registry.resolve(
                kotlin.jvm.internal.Reflection.typeOf(Class.forName(type.name, false, current.classLoader)),
            ) as Resolution.Codec).codec
            codec::deserialize
        }
    }
}

/** Reads a value of a v1 built-in. */
fun readBuiltIn(name: String, input: DataInput): Any = when (name) {
    "kotlin.Boolean" -> RecordFormat.readBoolean(input)
    "kotlin.Byte" -> RecordFormat.readByte(input)
    "kotlin.Short" -> RecordFormat.readShort(input)
    "kotlin.Int" -> RecordFormat.readInt(input)
    "kotlin.Long" -> RecordFormat.readLong(input)
    "kotlin.Float" -> RecordFormat.readFloat(input)
    "kotlin.Double" -> RecordFormat.readDouble(input)
    "kotlin.Char" -> RecordFormat.readChar(input)
    "kotlin.String" -> RecordFormat.readString(input)
    else -> throw IllegalArgumentException("$name isn't a built-in")
}

/**
 * Consumes values by walking their schema, without their classes: what a migration does with a removed value. It
 * reads every byte with the checks the codecs make, and the same budgets, so a malformed value fails here as it would
 * in a codec.
 *
 * Its work is bounded by the schema's validation (docs/persisted-format.md, "Resource limits"): it recurses at most
 * [PersistedSchema.MAX_DEPTH] levels, a collection's size is within its budget, and every value but the root's and a
 * sealed subtype's takes at least one byte, so each turn of a loop over a collection consumes input. It allocates
 * nothing for the values it skips.
 */
class SchemaWalker(private val schema: PersistedSchema) {
    /** Consumes a value of [type], a type with no type parameter, outside a record field: nullable means a marker. */
    fun skipValue(type: SchemaType.ClassType, input: DataInput) {
        if (!type.isNullable || RecordFormat.readNullMarker(input)) skipPresent(type, input)
    }

    /**
     * Consumes the value of a present [field] whose declared type is [type], in a declaration whose type parameters
     * are bound to [arguments]. The field's own `?` is the record's null bitmap. A field declared as a type parameter
     * is written by its argument's codec, so a nullable argument has its marker. When the field is declared `T?`, its
     * clear bitmap bit already says it holds a value, so that marker is `01`: `00` would be a second encoding of null.
     */
    fun skipField(type: SchemaType, arguments: Map<String, SchemaType>, input: DataInput, field: String) {
        when (type) {
            is SchemaType.TypeParameter -> {
                val argument = arguments.getValue(type.name) as SchemaType.ClassType
                if (type.isNullable && argument.isNullable) {
                    if (!RecordFormat.readNullMarker(input)) throw RecordFormat.nullInPresentField(field)
                    skipPresent(argument, input)
                } else {
                    skipValue(argument, input)
                }
            }
            is SchemaType.ClassType -> skipPresent(substitute(type, arguments), input)
        }
    }

    private fun skipPresent(type: SchemaType.ClassType, input: DataInput) {
        when (type.name) {
            "kotlin.String" -> RecordFormat.skipString(input)
            in PersistedSchema.BUILT_INS -> readBuiltIn(type.name, input)
            "kotlin.collections.List" -> repeat(RecordFormat.readCollectionSize(input)) { skipValue(type.argument(0), input) }
            "kotlin.collections.Map" -> repeat(RecordFormat.readCollectionSize(input)) {
                skipValue(type.argument(0), input)
                skipValue(type.argument(1), input)
            }
            else -> skipDeclared(schema.declaration(type.name), type, input)
        }
    }

    private fun skipDeclared(declaration: DeclarationSchema, type: SchemaType.ClassType, input: DataInput) {
        val arguments = declaration.typeParameters.zip(type.arguments.map { it.type!! }).toMap()
        when (declaration) {
            is RecordSchema -> {
                val fields = declaration.fields.sortedBy { it.name }
                val nulls = RecordFormat.readNullBitmap(input, fields.count { it.type.isNullable })
                var nullable = 0
                for (field in fields) {
                    val present = !field.type.isNullable || !nulls[nullable++]
                    if (present) skipField(field.type, arguments, input, "${declaration.typeName}.${field.name}")
                }
            }
            is EnumSchema -> {
                val id = input.readInt()
                if (declaration.constants.none { it.id == id }) {
                    throw MalformedDataException("${declaration.typeName} has no constant with ID $id")
                }
            }
            is SealedSchema -> {
                val id = input.readInt()
                val subtype = declaration.subtypes.firstOrNull { it.id == id }
                    ?: throw MalformedDataException("${declaration.typeName} has no subtype with ID $id")
                skipPresent(substitute(subtype.type, arguments), input)
            }
            is ObjectSchema -> Unit
            is ValueClassSchema -> throw UnsupportedOperationException("A value class's bytes are #18's decision")
        }
    }

    private fun SchemaType.ClassType.argument(index: Int): SchemaType.ClassType =
        arguments[index].type as SchemaType.ClassType
}
