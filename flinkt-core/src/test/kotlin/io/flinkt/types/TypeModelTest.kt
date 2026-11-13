package io.flinkt.types

import io.flinkt.assertImmutable
import io.flinkt.example.Color
import io.flinkt.example.Envelope
import io.flinkt.example.Event
import io.flinkt.example.Outer
import io.flinkt.example.Plain
import io.flinkt.example.Shutdown
import io.flinkt.example.StandInTypes
import io.flinkt.example.User
import io.flinkt.example.UserId
import io.flinkt.example.userSchema
import io.flinkt.registry.TypeRegistry
import io.flinkt.registry.model
import io.flinkt.schema.EnumSchema
import io.flinkt.schema.ObjectSchema
import io.flinkt.schema.RecordSchema
import io.flinkt.schema.SealedSchema
import io.flinkt.schema.ValueClassSchema
import kotlin.reflect.KType
import kotlin.reflect.KVariance
import kotlin.reflect.typeOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Structural analysis: what a `KType` becomes in the model, before anything is resolved. */
class TypeModelTest {
    private val registry = TypeRegistry.of(StandInTypes())

    private val user = User::class.java.name
    private val envelope = Envelope::class.java.name

    @Test
    fun `the model keeps every classifier, argument and nullability flag, and its canonical form shows them`() {
        val expected = mapOf(
            typeOf<Int>() to "kotlin.Int",
            typeOf<String>() to "kotlin.String",
            typeOf<String?>() to "kotlin.String?",
            typeOf<List<User>>() to "kotlin.collections.List<$user>",
            typeOf<List<User?>>() to "kotlin.collections.List<$user?>",
            typeOf<List<User>?>() to "kotlin.collections.List<$user>?",
            typeOf<Map<Long, User>>() to "kotlin.collections.Map<kotlin.Long, $user>",
            typeOf<Envelope<List<User?>>>() to "$envelope<kotlin.collections.List<$user?>>",
        )
        val models = expected.keys.map(registry::model)

        assertEquals(expected.values.toList(), models.map { it.toString() })
        assertEquals(models.size, models.toSet().size, "the models are pairwise different: $models")
        for (i in models.indices) {
            for (j in models.indices) if (i != j) assertNotEquals(models[i], models[j])
        }
    }

    @Test
    fun `a nested model keeps each level's classifier, shape and nullability`() {
        val model = registry.model<Envelope<List<User?>>>()

        assertEquals(envelope, model.classifierName)
        assertSame(Envelope::class.java, model.jvmClass)
        assertFalse(model.isNullable)
        assertInstanceOf(RecordSchema::class.java, (model.shape as TypeShape.Generated).schema)

        val list = model.arguments.single()
        assertEquals(KVariance.INVARIANT, list.variance)
        val listType = list.type!!
        assertEquals("kotlin.collections.List", listType.classifierName)
        assertEquals(TypeShape.Collection(CollectionKind.LIST), listType.shape)
        assertFalse(listType.isNullable)

        val element = listType.arguments.single().type!!
        assertEquals(user, element.classifierName)
        assertSame(User::class.java, element.jvmClass)
        assertTrue(element.isNullable)
        assertEquals(TypeShape.Generated(userSchema), element.shape)
        assertTrue(element.arguments.isEmpty())
    }

    @Test
    fun `equal KTypes give equal models`() {
        val first = registry.model(typeOf<Map<Long, List<User?>>>())
        val second = registry.model(typeOf<Map<Long, List<User?>>>())

        assertEquals(first, second)
        assertEquals(first.hashCode(), second.hashCode())
        assertEquals(registry.model<Envelope<User>>(), TypeRegistry.of(StandInTypes()).model<Envelope<User>>())
    }

    @Test
    fun `a model's arguments can't be changed at any depth`() {
        val model = registry.model<Map<Long, List<User?>>>()
        val canonical = "kotlin.collections.Map<kotlin.Long, kotlin.collections.List<$user?>>"

        assertImmutable(model.arguments)
        assertImmutable(model.arguments[1].type!!.arguments)
        assertEquals(canonical, model.toString())
        assertEquals(registry.model<Map<Long, List<User?>>>(), model)
        assertEquals(registry.model<Map<Long, List<User?>>>().hashCode(), model.hashCode())
    }

    @Test
    fun `built-ins are modeled by their Kotlin names and wrapper classes`() {
        val cases = listOf(
            typeOf<Boolean>() to BuiltInKind.BOOLEAN,
            typeOf<Byte>() to BuiltInKind.BYTE,
            typeOf<Short>() to BuiltInKind.SHORT,
            typeOf<Int>() to BuiltInKind.INT,
            typeOf<Long>() to BuiltInKind.LONG,
            typeOf<Float>() to BuiltInKind.FLOAT,
            typeOf<Double>() to BuiltInKind.DOUBLE,
            typeOf<Char>() to BuiltInKind.CHAR,
            typeOf<String>() to BuiltInKind.STRING,
        )
        for ((type, kind) in cases) {
            val model = registry.model(type)
            assertEquals(kind.kotlinName, model.toString())
            assertSame(kind.jvmClass, model.jvmClass)
            assertEquals(TypeShape.BuiltIn(kind), model.shape)
        }
        // `Int` and `Int?` share the wrapper class, and differ by nullability only.
        assertEquals(registry.model<Int>().jvmClass, registry.model<Int?>().jvmClass)
        assertNotEquals(registry.model<Int>(), registry.model<Int?>())
    }

    @Test
    @Suppress("REDUNDANT_PROJECTION") // Redundant for the compiler, but still part of the KType.
    fun `use-site variance and star projections are part of the type`() {
        val invariant = registry.model<List<User>>()
        val covariant = registry.model<List<out User>>()
        val contravariant = registry.model<Comparable<in User>>()
        val star = registry.model<List<*>>()

        assertEquals("kotlin.collections.List<out $user>", covariant.toString())
        assertEquals("kotlin.Comparable<in $user>", contravariant.toString())
        assertEquals("kotlin.collections.List<*>", star.toString())
        assertSame(TypeArgument.STAR, star.arguments.single())
        assertNull(star.arguments.single().type)
        assertEquals(KVariance.OUT, covariant.arguments.single().variance)
        assertNotEquals(invariant, covariant)
        assertNotEquals(invariant, star)
    }

    @Test
    fun `read-only and mutable collections are different types with the same JVM class`() {
        val readOnly = registry.model<List<User>>()
        val mutable = registry.model<MutableList<User>>()

        assertEquals("kotlin.collections.MutableList<$user>", mutable.toString())
        assertEquals(TypeShape.Collection(CollectionKind.MUTABLE_LIST), mutable.shape)
        assertSame(readOnly.jvmClass, mutable.jvmClass)
        assertNotEquals(readOnly, mutable)

        val kinds = mapOf(
            typeOf<Map<String, Int>>() to CollectionKind.MAP,
            typeOf<MutableMap<String, Int>?>() to CollectionKind.MUTABLE_MAP,
            typeOf<Set<Int>>() to CollectionKind.SET,
            typeOf<MutableSet<Int>>() to CollectionKind.MUTABLE_SET,
            typeOf<Collection<Int>>() to CollectionKind.COLLECTION,
            typeOf<MutableCollection<Int>>() to CollectionKind.MUTABLE_COLLECTION,
            typeOf<Iterable<Int>>() to CollectionKind.ITERABLE,
            typeOf<MutableIterable<Int>>() to CollectionKind.MUTABLE_ITERABLE,
            typeOf<Array<String>>() to CollectionKind.ARRAY,
            typeOf<IntArray>() to CollectionKind.PRIMITIVE_ARRAY,
        )
        for ((type, kind) in kinds) assertEquals(TypeShape.Collection(kind), registry.model(type).shape, "$type")
        assertEquals("kotlin.collections.MutableMap<kotlin.String, kotlin.Int>?", registry.model<MutableMap<String, Int>?>().toString())
        assertEquals("kotlin.Array<kotlin.String>", registry.model<Array<String>>().toString())
        assertEquals("kotlin.IntArray", registry.model<IntArray>().toString())
        assertEquals("kotlin.collections.MutableMap.MutableEntry<kotlin.String, kotlin.Int>", registry.model<MutableMap.MutableEntry<String, Int>>().toString())
        assertEquals("kotlin.collections.Map.Entry<kotlin.String, kotlin.Int>", registry.model<Map.Entry<String, Int>>().toString())
    }

    @Test
    fun `a platform type from Java is modeled as non-null, and a Java collection as the read-only collection`() {
        val platformString = kTypeOf(System.getProperty("java.version"))
        val platformList = kTypeOf(java.util.Collections.singletonList("a"))
        // The KTypes are platform types, unlike the Kotlin types the model equates them with.
        assertNotEquals(typeOf<String>(), platformString)
        assertNotEquals(typeOf<List<String>>(), platformList)
        assertNotEquals(typeOf<MutableList<String>>(), platformList)

        assertEquals(registry.model<String>(), registry.model(platformString))
        assertFalse(registry.model(platformString).isNullable)
        assertEquals(registry.model<List<String>>(), registry.model(platformList))
        assertEquals(TypeShape.Collection(CollectionKind.LIST), registry.model(platformList).shape)
    }

    @Test
    fun `a generated declaration's shape comes from its registered schema`() {
        assertInstanceOf(ValueClassSchema::class.java, generatedSchema<UserId>())
        assertInstanceOf(EnumSchema::class.java, generatedSchema<Color>())
        assertInstanceOf(SealedSchema::class.java, generatedSchema<Event>())
        assertInstanceOf(ObjectSchema::class.java, generatedSchema<Shutdown>())
        assertEquals(userSchema, generatedSchema<User>())
    }

    @Test
    fun `a class without registered metadata is modeled by its classifier only`() {
        val plain = registry.model<Plain>()
        assertSame(TypeShape.ClassifierOnly, plain.shape)
        assertEquals(Plain::class.java.name, plain.toString())

        // The same class without the module that registers it.
        val unregistered = TypeRegistry.of().model<User>()
        assertSame(TypeShape.ClassifierOnly, unregistered.shape)
        assertEquals(user, unregistered.toString())
        assertNotEquals(registry.model<User>(), unregistered)

        assertSame(TypeShape.ClassifierOnly, registry.model<Any>().shape)
        assertEquals("kotlin.Any", registry.model<Any>().toString())
    }

    @Test
    fun `nested classes are named with dots, and Nothing by its Kotlin name`() {
        assertEquals("io.flinkt.example.Outer.Inner", registry.model<Outer.Inner>().toString())
        assertInstanceOf(TypeShape.Generated::class.java, registry.model<Outer.Inner>().shape)

        val nothing = registry.model<List<Nothing>>().arguments.single().type!!
        assertEquals("kotlin.Nothing", nothing.toString())
        assertSame(TypeShape.ClassifierOnly, nothing.shape)
        assertEquals("kotlin.Nothing?", registry.model<List<Nothing?>>().arguments.single().type.toString())
    }

    @Test
    fun `a type parameter that isn't reified is modeled as one`() {
        val model = registry.model(Box<User>().listOfT())

        assertEquals("kotlin.collections.List<T>", model.toString())
        val parameter = model.arguments.single().type!!
        assertSame(TypeShape.TypeParameter, parameter.shape)
        assertNull(parameter.jvmClass)
        assertEquals("T?", registry.model(Box<User>().nullableT()).arguments.single().type.toString())
    }

    private inline fun <reified T> generatedSchema() = (registry.model<T>().shape as TypeShape.Generated).schema

    private class Box<T> {
        fun listOfT(): KType = typeOf<List<T>>()

        fun nullableT(): KType = typeOf<List<T?>>()
    }
}

/** The `KType` of [value]'s static type. A value from a Java method gives a platform type. */
inline fun <reified T> kTypeOf(@Suppress("UNUSED_PARAMETER") value: T): KType = typeOf<T>()
