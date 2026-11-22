package io.flinkt.ksp

import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSTypeAlias
import com.google.devtools.ksp.symbol.KSTypeArgument
import com.google.devtools.ksp.symbol.KSTypeParameter
import com.google.devtools.ksp.symbol.Variance
import java.security.MessageDigest

internal fun KSAnnotation.qualifiedName(): String? = annotationType.resolve().declaration.qualifiedName?.asString()

internal fun KSAnnotated.hasAnnotation(qualifiedName: String): Boolean =
    annotations.any { it.qualifiedName() == qualifiedName }

/** The name of [this] within its package, such as `User` or `Outer.Inner`. */
internal fun KSClassDeclaration.nestedName(): String =
    generateSequence(this) { it.parentDeclaration as? KSClassDeclaration }
        .toList()
        .reversed()
        .joinToString(".") { it.simpleName.asString() }

/** The JVM binary class name, matching `Class.getName()`, such as `com.example.User` or `com.example.Outer$Inner`. */
internal fun KSClassDeclaration.binaryName(): String {
    val pkg = packageName.asString()
    val nested = nestedName().replace('.', '$')
    return if (pkg.isEmpty()) nested else "$pkg.$nested"
}

/** Fully qualified Kotlin source reference, such as `com.example.Outer.Inner` or `User` in the unnamed package. */
internal fun KSClassDeclaration.kotlinReference(): String {
    val pkg = packageName.asString()
    val nested = nestedName().split('.').joinToString(".", transform = ::identifier)
    return if (pkg.isEmpty()) nested else "${qualified(pkg)}.$nested"
}

/**
 * Collision-free simple name for the generated codec file and class of [this] within its package.
 *
 * Uses an injective encoding over nesting segments (`_` -> `_0`, non-alphanumeric -> `_x<hex>_`, nesting `.` -> `_1`),
 * so `Outer.Inner` (`Outer_1Inner_FlinktCodec`) and `Outer_1Inner` (`Outer_01Inner_FlinktCodec`) can never collide,
 * while typical top-level names stay readable (`User_FlinktCodec`).
 */
internal fun KSClassDeclaration.codecSimpleName(): String {
    val mangled = nestedName().split('.').joinToString("_1") { segment ->
        buildString(segment.length) {
            for (ch in segment) {
                when {
                    ch == '_' -> append("_0")
                    ch in 'A'..'Z' || ch in 'a'..'z' || ch in '0'..'9' -> append(ch)
                    else -> {
                        append("_x")
                        val code = ch.code
                        append(HEX_DIGITS[(code ushr 12) and 0xf])
                        append(HEX_DIGITS[(code ushr 8) and 0xf])
                        append(HEX_DIGITS[(code ushr 4) and 0xf])
                        append(HEX_DIGITS[code and 0xf])
                        append('_')
                    }
                }
            }
        }
    }
    return "${mangled}_FlinktCodec"
}

/** The immediate superclass, excluding `kotlin.Any` and `java.lang.Object`. */
internal fun KSClassDeclaration.superclass(): KSClassDeclaration? = superTypes
    .mapNotNull { it.resolve().declaration as? KSClassDeclaration }
    .firstOrNull { it.classKind == ClassKind.CLASS && it.qualifiedName?.asString() !in ANY_NAMES }

/** Unwraps nested `typealias`es while preserving nullability from any alias layer. */
internal fun KSType.unwrapTypeAliases(resolver: Resolver): KSType {
    var current = this
    var nullable = current.isMarkedNullable
    val seen = HashSet<String>()
    while (current.declaration is KSTypeAlias) {
        val alias = current.declaration as KSTypeAlias
        val key = alias.qualifiedName?.asString() ?: alias.simpleName.asString()
        if (!seen.add(key)) break
        val expanded = alias.type.resolve()
        nullable = nullable || expanded.isMarkedNullable
        current = if (alias.typeParameters.isNotEmpty() && current.arguments.size == alias.typeParameters.size) {
            substitute(expanded, alias.typeParameters.zip(current.arguments).toMap(), resolver)
        } else {
            expanded
        }
    }
    return if (nullable && !current.isMarkedNullable) current.makeNullable() else current
}

private fun substitute(type: KSType, mapping: Map<KSTypeParameter, KSTypeArgument>, resolver: Resolver): KSType {
    val decl = type.declaration
    if (decl is KSTypeParameter) {
        val replacement = mapping[decl]?.type?.resolve() ?: return type
        return if (type.isMarkedNullable && !replacement.isMarkedNullable) replacement.makeNullable() else replacement
    }
    if (type.arguments.isEmpty()) return type
    var changed = false
    val replacedArgs = type.arguments.map { arg ->
        val argType = arg.type?.resolve() ?: return@map arg
        val newType = substitute(argType, mapping, resolver)
        if (newType == argType) {
            arg
        } else {
            changed = true
            resolver.getTypeArgument(resolver.createKSTypeReferenceFromKSType(newType), arg.variance)
        }
    }
    return if (changed) type.replace(replacedArgs) else type
}

/** Human-readable canonical type description for diagnostics, preserving arguments and `?`. */
internal fun describeType(type: KSType): String {
    if (type.isError) return type.toString()
    val decl = type.declaration
    val name = decl.qualifiedName?.asString() ?: decl.simpleName.asString()
    val args = if (type.arguments.isEmpty()) {
        ""
    } else {
        type.arguments.joinToString(prefix = "<", postfix = ">") { arg ->
            val argType = arg.type?.resolve()
            when {
                arg.variance == Variance.STAR || argType == null -> "*"
                arg.variance == Variance.COVARIANT -> "out ${describeType(argType)}"
                arg.variance == Variance.CONTRAVARIANT -> "in ${describeType(argType)}"
                else -> describeType(argType)
            }
        }
    }
    return name + args + if (type.isMarkedNullable) "?" else ""
}

/** Escapes a Kotlin identifier with backticks if it is a hard keyword or not a plain identifier. */
internal fun identifier(name: String): String =
    if (name in HARD_KEYWORDS || !IDENTIFIER.matches(name)) "`$name`" else name

/** Escapes each segment of a dot-separated qualified name. */
internal fun qualified(name: String): String = name.split('.').joinToString(".", transform = ::identifier)

/** Renders [value] as a Kotlin string literal, escaping `$`, `\`, `"`, and control characters. */
internal fun stringLiteral(value: String): String = buildString(value.length + 2) {
    append('"')
    for (ch in value) {
        when (ch) {
            '\\' -> append("\\\\")
            '"' -> append("\\\"")
            '$' -> append("\\$")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(ch)
        }
    }
    append('"')
}

/** First 16 lowercase hex chars of SHA-256 over [values], separated by NUL bytes. */
internal fun sha256Hex16(values: List<String>): String {
    val digest = MessageDigest.getInstance("SHA-256")
    for (value in values) {
        digest.update(value.toByteArray(Charsets.UTF_8))
        digest.update(0)
    }
    val bytes = digest.digest()
    return buildString(16) {
        for (i in 0 until 8) {
            val b = bytes[i].toInt() and 0xff
            append(HEX_DIGITS[b ushr 4])
            append(HEX_DIGITS[b and 0xf])
        }
    }
}

private const val HEX_DIGITS: String = "0123456789abcdef"

private val ANY_NAMES: Set<String> = setOf("kotlin.Any", "java.lang.Object")

private val IDENTIFIER: Regex = Regex("[A-Za-z_][A-Za-z0-9_]*")

private val HARD_KEYWORDS: Set<String> = setOf(
    "as", "break", "class", "continue", "do", "else", "false", "for", "fun", "if", "in", "interface", "is", "null",
    "object", "package", "return", "super", "this", "throw", "true", "try", "typealias", "typeof", "val", "var",
    "when", "while",
)
