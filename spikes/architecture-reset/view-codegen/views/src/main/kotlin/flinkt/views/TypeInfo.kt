// SPIKE: minimal typeInfo; the forwarding spike needs only Long and String.
package flinkt.views

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import kotlin.reflect.KType
import kotlin.reflect.typeOf

public class UnsupportedFlinktType(message: String) : IllegalArgumentException(message)

@PublishedApi
internal fun resolve(type: KType): TypeInformation<*> = when {
    type.isMarkedNullable -> throw UnsupportedFlinktType("$type is nullable")
    type.classifier == Long::class -> Types.LONG
    type.classifier == String::class -> Types.STRING
    else -> throw UnsupportedFlinktType("$type is not modeled")
}

@Suppress("UNCHECKED_CAST")
public inline fun <reified T> typeInfo(): TypeInformation<T> = resolve(typeOf<T>()) as TypeInformation<T>
