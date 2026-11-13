package io.flinkt.codegen

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSTypeArgument
import com.google.devtools.ksp.symbol.KSTypeParameter
import com.google.devtools.ksp.symbol.Variance

/**
 * Writes Flink's types as Kotlin source in a generated member, every class fully qualified.
 *
 * [classScope] gives each type parameter of the Flink class and its supertypes in the view's terms (keyed by
 * [key]). The member's own type parameters, [methodTypeParameters], keep their names, unless one is among [reserved],
 * the view's own type parameter names, which would shadow it, and then gets a numbered name.
 *
 * A Java collection is written as Kotlin's read-only type, at any depth: a returned collection comes back read-only,
 * and a parameter accepts a read-only and a mutable collection alike.
 */
internal class Scope(
    private val classScope: Map<String, String>,
    private val methodTypeParameters: List<KSTypeParameter>,
    reserved: List<String>,
) {
    private val methodNames: Map<String, String> = buildMap {
        val taken = reserved.toMutableSet()
        for (parameter in methodTypeParameters) {
            val name = parameter.name.asString()
            val unique = if (taken.add(name)) name else generateSequence(1) { it + 1 }.map { "$name$it" }.first(taken::add)
            put(name, unique)
        }
    }

    fun render(type: KSType): String {
        val nullable = if (type.isMarkedNullable) "?" else ""
        return when (val declaration = type.declaration) {
            is KSTypeParameter -> typeParameter(declaration) + nullable
            is KSClassDeclaration -> {
                val name = declaration.qualifiedName()
                qualified(READ_ONLY[name] ?: name) + arguments(type) + nullable
            }
            else -> throw IllegalStateException("Can't write the type $type of ${declaration.qualifiedName()}")
        }
    }

    /** [type]'s type arguments, such as `<T, *>`, or nothing. A raw Java type gets star projections. */
    fun arguments(type: KSType): String {
        val parameters = (type.declaration as? KSClassDeclaration)?.typeParameters.orEmpty()
        if (parameters.isEmpty()) return ""
        if (type.arguments.size != parameters.size) return parameters.joinToString(prefix = "<", postfix = ">") { "*" }
        return type.arguments.joinToString(prefix = "<", postfix = ">", transform = ::argument)
    }

    /** The member's type parameters, such as `K, W : org.apache.flink…Window`, with a single bound written inline. */
    fun declarations(): String = methodTypeParameters.joinToString { parameter ->
        val bounds = bounds(parameter)
        methodNames.getValue(parameter.name.asString()) + (bounds.singleOrNull()?.let { ": $it" } ?: "")
    }

    /** The `where` constraints of a type parameter with more than one bound. */
    fun constraints(): String = methodTypeParameters
        .filter { bounds(it).size > 1 }
        .flatMap { parameter -> bounds(parameter).map { "${methodNames.getValue(parameter.name.asString())} : $it" } }
        .joinToString()

    private fun bounds(parameter: KSTypeParameter): List<String> =
        parameter.bounds.map { it.resolve() }.filterNot { it.isAny() }.map(::render).toList()

    private fun argument(argument: KSTypeArgument): String {
        val type = argument.type?.resolve() ?: return "*"
        return when (argument.variance) {
            Variance.STAR -> "*"
            Variance.COVARIANT -> "out " + render(type)
            Variance.CONTRAVARIANT -> "in " + render(type)
            Variance.INVARIANT -> render(type)
        }
    }

    private fun typeParameter(parameter: KSTypeParameter): String =
        if (parameter.parentDeclaration is KSFunctionDeclaration) {
            methodNames[parameter.name.asString()] ?: throw IllegalStateException("Unknown type parameter ${parameter.name.asString()}")
        } else {
            classScope[parameter.key()] ?: throw IllegalStateException("Unknown type parameter ${parameter.key()}")
        }

    private companion object {
        val READ_ONLY = mapOf(
            "kotlin.collections.MutableIterable" to "kotlin.collections.Iterable",
            "kotlin.collections.MutableCollection" to "kotlin.collections.Collection",
            "kotlin.collections.MutableList" to "kotlin.collections.List",
            "kotlin.collections.MutableSet" to "kotlin.collections.Set",
            "kotlin.collections.MutableMap" to "kotlin.collections.Map",
            "kotlin.collections.MutableMap.MutableEntry" to "kotlin.collections.Map.Entry",
            "kotlin.collections.MutableIterator" to "kotlin.collections.Iterator",
            "kotlin.collections.MutableListIterator" to "kotlin.collections.ListIterator",
        )
    }
}

/** A class's type parameter, as [Scope]'s class scope keys it: its class and its name. */
internal fun KSTypeParameter.key(): String = "${parentDeclaration?.qualifiedName()}#${name.asString()}"

internal fun KSDeclaration.qualifiedName(): String =
    qualifiedName?.asString() ?: throw IllegalStateException("${simpleName.asString()} has no qualified name")

internal fun KSAnnotation.qualifiedName(): String? = annotationType.resolve().declaration.qualifiedName?.asString()

internal fun KSAnnotation.argument(name: String): Any? = arguments.firstOrNull { it.name?.asString() == name }?.value

internal fun KSAnnotated.hasAnnotation(qualifiedName: String): Boolean = annotations.any { it.qualifiedName() == qualifiedName }

/** Whether an annotation of that simple name is present, such as any of the `@Nullable` annotations Flink uses. */
internal fun KSAnnotated.hasAnnotationNamed(simpleName: String): Boolean = annotations.any { it.shortName.asString() == simpleName }

/** The JVM binary name, such as `org.apache.flink.streaming.api.datastream.DataStream$Collector`. */
internal fun KSClassDeclaration.binaryName(): String {
    val packageName = packageName.asString()
    return (if (packageName.isEmpty()) "" else "$packageName.") + nestedName().replace('.', '$')
}

/** The name within the package, such as `DataStream.Collector`. */
internal fun KSClassDeclaration.nestedName(): String =
    generateSequence(this) { it.parentDeclaration as? KSClassDeclaration }.toList().reversed().joinToString(".") { it.simpleName.asString() }

/** The superclass, not counting `Any`. */
internal fun KSClassDeclaration.superclass(): KSClassDeclaration? = superTypes
    .mapNotNull { it.resolve().declaration as? KSClassDeclaration }
    .firstOrNull { it.classKind == ClassKind.CLASS && it.qualifiedName?.asString() !in ANY }

/** The number of superclasses, not counting `Any`. */
internal fun KSClassDeclaration.depth(): Int = generateSequence(superclass()) { it.superclass() }.count()

/**
 * Whether this is a Flink stream type, or a type that builds one, such as `WindowedStream`, `BroadcastStream` or
 * `CoGroupedStreams`: a `DataStream`, or any class of Flink's stream API package, nested ones included. The exception
 * is `DataStreamSink`. A sink ends a chain: its methods configure it, and no stream comes out of it.
 */
internal fun KSClassDeclaration.isStreamType(): Boolean {
    val outermost = generateSequence(this) { it.parentDeclaration as? KSClassDeclaration }.last()
    if (outermost.qualifiedName() == DATA_STREAM_SINK) return false
    return packageName.asString() == STREAM_PACKAGE ||
        qualifiedName() == DATA_STREAM ||
        getAllSuperTypes().any { it.declaration.qualifiedName?.asString() == DATA_STREAM }
}

/**
 * Whether [this] is Flink's `TypeInformation`, or a subclass of it. A stream's method that returns one reads a type
 * of that stream, as `DataStream.getType()` reads its output type.
 */
internal fun KSType.isTypeInformation(): Boolean {
    val declaration = declaration as? KSClassDeclaration ?: return false
    return declaration.qualifiedName() == TYPE_INFORMATION ||
        declaration.getAllSuperTypes().any { it.declaration.qualifiedName?.asString() == TYPE_INFORMATION }
}

/** Whether [this] mentions a type parameter of the method, named in [names], at any depth. */
internal fun KSType.mentions(names: Set<String>): Boolean {
    val declaration = declaration
    if (declaration is KSTypeParameter && declaration.parentDeclaration is KSFunctionDeclaration && declaration.name.asString() in names) {
        return true
    }
    return arguments.any { it.type?.resolve()?.mentions(names) == true }
}

/** Whether [this], or a type argument at any depth, couldn't be resolved. */
internal fun KSType.hasError(): Boolean = isError || arguments.any { it.type?.resolve()?.hasError() == true }

internal fun KSType.isAny(): Boolean = declaration.qualifiedName?.asString() in ANY

/** The JVM array dimensions of [this]: 0 for a non-array, 1 for `IntArray` or `Array<String>`, 2 for `Array<IntArray>`. */
internal fun KSType.arrayDimensions(): Int {
    val name = declaration.qualifiedName?.asString()
    return when {
        name == "kotlin.Array" -> 1 + (arguments.single().type?.resolve()?.arrayDimensions() ?: 0)
        name in PRIMITIVE_ARRAYS -> 1
        else -> 0
    }
}

/** A Kotlin identifier, in backticks if it's a hard keyword or has a character Kotlin doesn't allow, such as `$`. */
internal fun identifier(name: String): String =
    if (name in HARD_KEYWORDS || !IDENTIFIER.matches(name)) "`$name`" else name

/** A qualified name, each segment an [identifier]. */
internal fun qualified(name: String): String = name.split('.').joinToString(".", transform = ::identifier)

/** [type] for a message: simple names, such as `MapStateDescriptor<*, *>`. */
internal fun describe(type: KSType): String {
    val declaration = type.declaration
    val name = (declaration as? KSClassDeclaration)?.nestedName() ?: declaration.simpleName.asString()
    val arguments = if (type.arguments.isEmpty()) "" else type.arguments.joinToString(prefix = "<", postfix = ">") { argument ->
        val argumentType = argument.type?.resolve()
        when {
            argument.variance == Variance.STAR || argumentType == null -> "*"
            argument.variance == Variance.COVARIANT -> "out " + describe(argumentType)
            argument.variance == Variance.CONTRAVARIANT -> "in " + describe(argumentType)
            else -> describe(argumentType)
        }
    }
    return name + arguments + if (type.isMarkedNullable) "?" else ""
}

private const val STREAM_PACKAGE = "org.apache.flink.streaming.api.datastream"
private const val DATA_STREAM = "$STREAM_PACKAGE.DataStream"
private const val DATA_STREAM_SINK = "$STREAM_PACKAGE.DataStreamSink"
private const val TYPE_INFORMATION = "org.apache.flink.api.common.typeinfo.TypeInformation"
private val ANY = setOf("kotlin.Any", "java.lang.Object")

private val PRIMITIVE_ARRAYS = setOf(
    "kotlin.BooleanArray", "kotlin.ByteArray", "kotlin.CharArray", "kotlin.ShortArray", "kotlin.IntArray",
    "kotlin.LongArray", "kotlin.FloatArray", "kotlin.DoubleArray",
)

private val IDENTIFIER = Regex("[A-Za-z_][A-Za-z0-9_]*")

private val HARD_KEYWORDS = setOf(
    "as", "break", "class", "continue", "do", "else", "false", "for", "fun", "if", "in", "interface", "is", "null",
    "object", "package", "return", "super", "this", "throw", "true", "try", "typealias", "typeof", "val", "var",
    "when", "while",
)
