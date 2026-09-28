package neg
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.ValueStateDescriptor
import spike.model.User
import spike.state.valueState
fun check(ctx: RuntimeContext) {
    val d: ValueStateDescriptor<User> = ctx.valueState("user")   // must fail: valueState binds state, it doesn't build a descriptor
}
