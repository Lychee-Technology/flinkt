package io.flinkt.testing.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration

/**
 * For each class annotated `@sample.Greet`, generates `<Name>Greeting.TEXT`. It reports an error, located at the
 * declaration, for anything else annotated `@Greet`.
 */
class GreetingProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        GreetingProcessor(environment.codeGenerator, environment.logger, environment.options, environment.kotlinVersion)
}

private class GreetingProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
    private val options: Map<String, String>,
    private val kotlinVersion: KotlinVersion,
) : SymbolProcessor {
    private var warned = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (!warned && options["logKotlinVersion"] == "true") {
            warned = true
            logger.warn("ksp kotlinVersion=$kotlinVersion")
        }
        for (symbol in resolver.getSymbolsWithAnnotation("sample.Greet")) {
            val declaration = symbol as? KSClassDeclaration
            if (declaration == null || declaration.classKind != ClassKind.CLASS) {
                logger.error("@Greet applies to classes only", symbol)
                continue
            }
            val name = declaration.simpleName.asString()
            val pkg = declaration.packageName.asString()
            codeGenerator.createNewFile(Dependencies(false, declaration.containingFile!!), pkg, "${name}Greeting")
                .bufferedWriter()
                .use { it.write("package $pkg\n\npublic object ${name}Greeting {\n    public const val TEXT: String = \"Hello, $name\"\n}\n") }
        }
        return emptyList()
    }
}
