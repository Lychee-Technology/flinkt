package io.flinkt.ksp

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider

/**
 * Entry point for Flinkt's KSP processor.
 *
 * Registered in `META-INF/services/com.google.devtools.ksp.processing.SymbolProcessorProvider`. Generates Flink-free
 * codecs, persisted schema metadata, and the compilation's `GeneratedTypeModule` for `@FlinkType` declarations.
 */
public class FlinktSymbolProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor =
        FlinktSymbolProcessor(
            codeGenerator = environment.codeGenerator,
            logger = environment.logger,
            kotlinVersion = environment.kotlinVersion,
        )
}
