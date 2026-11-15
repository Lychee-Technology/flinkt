package io.flinkt.flink.format

import io.flinkt.codec.GeneratedCodec
import io.flinkt.registry.Resolution
import io.flinkt.registry.TypeRegistry
import io.flinkt.schema.DeclarationSchema
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.FieldSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SchemaType
import io.flinkt.schema.SchemaTypeArgument
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.ValueClassSchema
import io.flinkt.types.UnsupportedTypeException
import java.io.DataInput
import java.io.IOException
import java.util.Arrays
import java.util.TreeSet
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
 * each role and under a `Map` key, a changed persisted order, fields removed from or added to the root record, and
 * type changes. #34 plans every shape.
 *
 * A record's two orders are compared separately (docs/persisted-format.md, "Field order"). Its persisted order is its
 * layout: a change to it is a field permutation, which no stage migrates yet, so it's incompatible. Its constructor
 * order decides no byte, but a data class's generated `hashCode()` follows it, so a change to it alone is as-is
 * except where what a release persisted depends on a hash code: in a partition key, whose key groups follow it, and
 * in a record under a `Map` key, whose keys a codec counts by hash code where the map is written and where it's read.
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
        // Records with the same fields in both schemas: a change of either order is checked first, by itself.
        val sameFields = old.declarations.values.filterIsInstance<RecordSchema>().mapNotNull { record ->
            val now = current.declarations[record.typeName] as? RecordSchema
            if (now != null && now.fields.toSet() == record.fields.toSet()) record to now else null
        }
        for ((before, now) in sameFields) {
            if (before.persistedFields != now.persistedFields) return incompatible(persistedOrderChanged(before, now))
        }
        val counted by lazy { MapKeys.countedRecords(old) + MapKeys.countedRecords(current) }
        for ((before, now) in sameFields) {
            if (before.fields == now.fields) continue
            val change = "the constructor order of ${before.typeName} changed from (${names(before.fields)}) to " +
                "(${names(now.fields)}). Its bytes didn't change, but a data class's generated hashCode() follows " +
                "constructor order"
            if (KeyRole.PARTITION_KEY in roles) return incompatible("$change, so the partition key's key groups would move")
            if (before.typeName in counted) {
                return incompatible(
                    "$change, and ${before.typeName} is in a Map key, whose keys are counted by hash code where the " +
                        "map is written and where it's read. A map written within the budget of " +
                        "${RecordFormat.MAX_KEYS_PER_HASH_CODE} keys per hash code could be over it when it's read",
                )
            }
        }
        if (old.sameLayout(current)) return Plan.AsIs
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
        // The fields that stay are written in the old persisted order: the same as the current one, or a permutation.
        val kept = oldRecord.persistedFields.filter { it.name in currentFields }
        if (kept.map { it.name } != currentRecord.persistedFields.map { it.name }) {
            return incompatible(persistedOrderChanged(oldRecord, currentRecord))
        }
        val operations = LinkedHashMap<String, FieldOperation>()
        for (field in oldRecord.persistedFields) {
            val now = currentFields[field.name]
            operations[field.name] = when {
                now == null -> FieldOperation.REMOVE
                now.type == field.type && sameLayouts(reachable(old, field.type), reachable(current, now.type)) -> FieldOperation.KEEP
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

    private fun names(fields: List<FieldSchema>) = fields.joinToString { it.name }

    private fun persistedOrderChanged(old: RecordSchema, current: RecordSchema): String =
        "the persisted order of ${old.typeName} changed from (${names(old.persistedFields)}) to " +
            "(${names(current.persistedFields)}). Its records are written in that order, and reading them in another " +
            "is a field permutation, which no migration stage performs. Without @PersistedFieldOrder, the persisted " +
            "order is the constructor order: to reorder the constructor and keep the layout, give each property a " +
            "@PersistedFieldOrder in the old order"

    private fun difference(old: PersistedSchema, current: PersistedSchema): String {
        if (old.root != current.root) return "the serialized type changed from ${old.root} to ${current.root}"
        val names = (old.declarations.keys + current.declarations.keys).sorted()
        val name = names.first { !PersistedSchema.sameLayout(old.declarations[it], current.declarations[it]) }
        return "the declaration of $name changed. Old: ${old.declarations[name] ?: "none"}. Current: ${current.declarations[name] ?: "none"}"
    }

    private fun sameLayouts(a: Map<String, DeclarationSchema>, b: Map<String, DeclarationSchema>): Boolean =
        a.keys == b.keys && a.all { (name, declaration) -> PersistedSchema.sameLayout(declaration, b[name]) }

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
    private val persistedFields = oldRecord.persistedFields
    private val nullableFields = persistedFields.count { it.type.isNullable }

    @Suppress("UNCHECKED_CAST")
    private val constructor = current.codec as? MigrationConstructor<T>
        ?: throw IncompatibleStateException("${old.root}'s codec has no migration constructor")
    private val walker = SchemaWalker(old)

    // Per old field, in the old persisted order, decided once: how it's read, and for a kept one, its current
    // constructor parameter, found by name.
    private val readers: List<((DataInput) -> Any?)?> = persistedFields.map { field ->
        when (plan.operations.getValue(field.name)) {
            FieldOperation.KEEP -> reader(field.type as SchemaType.ClassType)
            FieldOperation.REMOVE -> null
        }
    }
    private val parameters = IntArray(persistedFields.size) { constructor.parameterNames.indexOf(persistedFields[it].name) }

    override fun deserialize(source: DataInputView): T {
        val nulls = RecordFormat.readNullBitmap(source, nullableFields)
        val arguments = arrayOfNulls<Any?>(constructor.parameterNames.size)
        var nullable = 0
        for ((i, field) in persistedFields.withIndex()) {
            val present = !field.type.isNullable || !nulls[nullable++]
            val read = readers[i]
            when {
                read != null -> arguments[parameters[i]] = if (present) read(source) else null
                present -> walker.skipField(old.root, field, source)
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
 * in a codec, a `Map` that holds a key twice included.
 *
 * Its work is linear in the bytes it consumes, but for a map's keys, each of which costs at most `log2` of the map's
 * size in comparisons (docs/persisted-format.md, "Resource limits"). It recurses at most [PersistedSchema.MAX_DEPTH]
 * levels, a collection's size is within its budget, and every value but the root's and a sealed subtype's takes at
 * least one byte, so each turn of a loop over a collection consumes input. Each declaration is prepared once, the
 * first time a value of it is read, so no value costs work for its schema's width: a type argument is looked up in
 * the scope it was written in, never substituted, and an enum or sealed ID is found by binary search. The only thing
 * it keeps for a value it consumes is a map's keys' bytes, until the map ends.
 */
class SchemaWalker(private val schema: PersistedSchema) {
    private val prepared = HashMap<String, Prepared>()

    /** Consumes a value of [type], a type with no type parameter, outside a record field: nullable means a marker. */
    fun skipValue(type: SchemaType.ClassType, input: DataInput) = skipValue(type, null, input, inKey = false)

    /** Consumes the value of [field], a present field of [owner], which is a record type with no type parameter. */
    fun skipField(owner: SchemaType.ClassType, field: FieldSchema, input: DataInput) =
        skipField(field, owner.name, scope(prepare(owner.name), owner, null), input, inKey = false)

    /** Consumes a value of [type], used in [scope]. A nullable type, or a nullable argument, means a marker. */
    private fun skipValue(type: SchemaType, scope: Scope?, input: DataInput, inKey: Boolean) =
        resolve(type, scope) { classType, where, nullableArgument ->
            if (!(type.isNullable || nullableArgument) || RecordFormat.readNullMarker(input)) {
                skipPresent(classType, where, input, inKey)
            }
        }

    /**
     * Consumes the value of a present [field] of [owner], whose type arguments are [scope]'s. The field's own `?` is
     * the record's null bitmap. A field declared as a type parameter is written by its argument's codec, so a nullable
     * argument has its marker. When the field is declared `T?`, its clear bitmap bit already says it holds a value, so
     * that marker is `01`: `00` would be a second encoding of null.
     */
    private fun skipField(field: FieldSchema, owner: String, scope: Scope?, input: DataInput, inKey: Boolean) {
        when (val type = field.type) {
            is SchemaType.TypeParameter -> resolve(type, scope) { argument, where, nullableArgument ->
                if (type.isNullable && nullableArgument) {
                    if (!RecordFormat.readNullMarker(input)) throw RecordFormat.nullInPresentField("$owner.${field.name}")
                    skipPresent(argument, where, input, inKey)
                } else if (!nullableArgument || RecordFormat.readNullMarker(input)) {
                    skipPresent(argument, where, input, inKey)
                }
            }
            is SchemaType.ClassType -> skipPresent(type, scope, input, inKey)
        }
    }

    /** Consumes a present value of [type], whose type parameters are [scope]'s. [inKey] is set inside a map key. */
    private fun skipPresent(type: SchemaType.ClassType, scope: Scope?, input: DataInput, inKey: Boolean) {
        when (type.name) {
            "kotlin.String" -> RecordFormat.skipString(input)
            in PersistedSchema.BUILT_INS -> skipBuiltIn(type.name, input)
            LIST -> {
                if (inKey) throw collectionInKey(type)
                val element = type.arguments[0].type!!
                repeat(RecordFormat.readCollectionSize(input)) { skipValue(element, scope, input, inKey = false) }
            }
            MAP -> {
                if (inKey) throw collectionInKey(type)
                skipMap(type.arguments[0].type!!, type.arguments[1].type!!, scope, input)
            }
            else -> skipDeclared(prepare(type.name), type, scope, input, inKey)
        }
    }

    /**
     * A map's entries, checking that no key is there twice without the key's class. A key's equality depends only on
     * what's written (docs/architecture.md, "Map keys"), and each of its encodings is canonical, so two keys are
     * equal exactly when their bytes are (docs/persisted-format.md, "Deterministic bytes").
     *
     * The map's keys' bytes are kept until it ends, in a set ordered by those bytes: at most the bytes they were read
     * from, plus a constant per entry, as a codec's decoded map holds its keys. A key costs at most `log2(size)`
     * comparisons, 26 at the size budget, each of at most its own bytes, whatever the keys are: no hash code that
     * keys could be written to share.
     */
    private fun skipMap(key: SchemaType, value: SchemaType, scope: Scope?, input: DataInput) {
        val size = RecordFormat.readCollectionSize(input)
        if (size == 0) return
        val recorder = KeyRecorder(input)
        val keys = TreeSet(BYTE_ORDER)
        repeat(size) {
            recorder.start()
            skipValue(key, scope, recorder, inKey = true)
            val bytes = recorder.bytes()
            if (!keys.add(bytes)) throw MalformedDataException("a map holds the key written as ${hexPrefix(bytes)} twice")
            skipValue(value, scope, input, inKey = false)
        }
    }

    private fun skipDeclared(declaration: Prepared, type: SchemaType.ClassType, scope: Scope?, input: DataInput, inKey: Boolean) {
        // The declaration's body is read in a scope of its own: its type parameters stand for type's arguments.
        val body = scope(declaration, type, scope)
        when (val schema = declaration.schema) {
            is RecordSchema -> {
                val nulls = RecordFormat.readNullBitmap(input, declaration.nullableFields)
                var nullable = 0
                for (field in declaration.fields) {
                    val present = !field.type.isNullable || !nulls[nullable++]
                    if (present) skipField(field, schema.typeName, body, input, inKey)
                }
            }
            is EnumSchema -> {
                val id = input.readInt()
                if (Arrays.binarySearch(declaration.ids, id) < 0) {
                    throw MalformedDataException("${schema.typeName} has no constant with ID $id")
                }
            }
            is SealedSchema -> {
                val id = input.readInt()
                val index = Arrays.binarySearch(declaration.ids, id)
                if (index < 0) throw MalformedDataException("${schema.typeName} has no subtype with ID $id")
                skipPresent(declaration.subtypes[index], body, input, inKey)
            }
            is ObjectSchema -> Unit
            is ValueClassSchema -> throw UnsupportedOperationException("A value class's bytes are #18's decision")
        }
    }

    private fun prepare(name: String): Prepared = prepared.getOrPut(name) { Prepared(schema.declaration(name)) }

    /** The scope [declaration]'s body is read in, for a value of [type], whose arguments are written in [outer]. */
    private fun scope(declaration: Prepared, type: SchemaType.ClassType, outer: Scope?): Scope? =
        if (declaration.parameters.isEmpty()) null else Scope(declaration, type.arguments, outer)

    /**
     * Calls [use] with the class type that [type], used in [scope], stands for, the scope that class type's own
     * arguments are written in, and whether an argument on the way is nullable, as substituting it would make it. A
     * type parameter's argument is written in the scope around the one that declares it, so a chain of them is
     * followed outwards, one scope per step, never deeper than the nesting.
     */
    private inline fun <R> resolve(type: SchemaType, scope: Scope?, use: (SchemaType.ClassType, Scope?, Boolean) -> R): R {
        var current = type
        var where = scope
        var nullable = false
        while (current is SchemaType.TypeParameter) {
            val declaring = checkNotNull(where) { "${current.name} is used outside a generic declaration" }
            current = checkNotNull(declaring.arguments[declaring.declaration.parameters.getValue(current.name)].type)
            where = declaring.outer
            nullable = nullable || current.isNullable
        }
        return use(current as SchemaType.ClassType, where, nullable)
    }

    private fun skipBuiltIn(name: String, input: DataInput) {
        when (name) {
            "kotlin.Boolean" -> RecordFormat.readBoolean(input)
            "kotlin.Byte" -> RecordFormat.readByte(input)
            "kotlin.Short" -> RecordFormat.readShort(input)
            "kotlin.Int" -> RecordFormat.readInt(input)
            "kotlin.Long" -> RecordFormat.readLong(input)
            "kotlin.Float" -> RecordFormat.readFloat(input)
            "kotlin.Double" -> RecordFormat.readDouble(input)
            "kotlin.Char" -> RecordFormat.readChar(input)
            "kotlin.String" -> RecordFormat.skipString(input)
            else -> throw IllegalArgumentException("$name isn't a built-in")
        }
    }

    // No key type holds a collection (docs/architecture.md, "Map keys"), so no release wrote one, and a Map's bytes
    // follow its iteration order, so two equal keys holding one could have different bytes.
    private fun collectionInKey(type: SchemaType.ClassType) = MalformedDataException(
        "a map key holds a ${type.name}, and no key type holds a collection, so no release wrote it",
    )

    /** A declaration of the old schema, prepared to read its values. */
    private class Prepared(val schema: DeclarationSchema) {
        /** Each type parameter's position. */
        val parameters: Map<String, Int> = schema.typeParameters.withIndex().associate { (index, name) -> name to index }

        /** A record's fields, in its persisted order, and how many of them are nullable. */
        val fields: List<FieldSchema> = (schema as? RecordSchema)?.persistedFields.orEmpty()
        val nullableFields: Int = fields.count { it.type.isNullable }

        /** An enum's constant IDs, or a sealed type's subtype IDs, in ascending order, and the subtypes in that order. */
        val ids: IntArray
        val subtypes: List<SchemaType.ClassType>

        init {
            when (schema) {
                is EnumSchema -> {
                    ids = schema.constants.map { it.id }.sorted().toIntArray()
                    subtypes = emptyList()
                }
                is SealedSchema -> {
                    val sorted = schema.subtypes.sortedBy { it.id }
                    ids = sorted.map { it.id }.toIntArray()
                    subtypes = sorted.map { it.type }
                }
                else -> {
                    ids = IntArray(0)
                    subtypes = emptyList()
                }
            }
        }
    }

    /** The arguments a generic declaration's value has: [arguments], written in [outer]. */
    private class Scope(val declaration: Prepared, val arguments: List<SchemaTypeArgument>, val outer: Scope?)

    /** A [DataInput] that keeps a copy of the bytes a map key is read from, so it can be compared without its class. */
    private class KeyRecorder(private val input: DataInput) : DataInput {
        private var bytes = ByteArray(16)
        private var length = 0

        fun start() {
            length = 0
        }

        fun bytes(): ByteArray = bytes.copyOf(length)

        private fun record(value: Long, count: Int) {
            if (length + count > bytes.size) bytes = bytes.copyOf(maxOf(bytes.size * 2, length + count))
            for (shift in count - 1 downTo 0) bytes[length++] = (value ushr (8 * shift)).toByte()
        }

        override fun readBoolean(): Boolean = readUnsignedByte() != 0

        override fun readByte(): Byte = input.readByte().also { record(it.toLong(), 1) }

        override fun readUnsignedByte(): Int = input.readUnsignedByte().also { record(it.toLong(), 1) }

        override fun readShort(): Short = input.readShort().also { record(it.toLong(), 2) }

        override fun readUnsignedShort(): Int = input.readUnsignedShort().also { record(it.toLong(), 2) }

        override fun readChar(): Char = input.readChar().also { record(it.code.toLong(), 2) }

        override fun readInt(): Int = input.readInt().also { record(it.toLong(), 4) }

        override fun readLong(): Long = input.readLong().also { record(it, 8) }

        override fun readFloat(): Float = java.lang.Float.intBitsToFloat(readInt())

        override fun readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

        override fun readFully(b: ByteArray) = readFully(b, 0, b.size)

        override fun readFully(b: ByteArray, off: Int, len: Int) {
            input.readFully(b, off, len)
            for (i in off until off + len) record(b[i].toLong(), 1)
        }

        override fun skipBytes(n: Int): Int = throw UnsupportedOperationException("a key's bytes are read, to compare them")

        override fun readLine(): String = throw UnsupportedOperationException()

        override fun readUTF(): String = throw UnsupportedOperationException()
    }

    private companion object {
        const val LIST = "kotlin.collections.List"
        const val MAP = "kotlin.collections.Map"

        /** Unsigned lexicographic order: equal exactly when the bytes are. */
        val BYTE_ORDER = Comparator<ByteArray> { a, b ->
            var i = 0
            while (i < a.size && i < b.size && a[i] == b[i]) i++
            if (i < a.size && i < b.size) (a[i].toInt() and 0xff) - (b[i].toInt() and 0xff) else a.size - b.size
        }

        /** At most the first 16 bytes, so a long key doesn't make a long message. */
        fun hexPrefix(bytes: ByteArray): String = bytes.take(16).joinToString(" ") { "%02x".format(it) } +
            if (bytes.size > 16) " … (${bytes.size} bytes)" else ""
    }
}
