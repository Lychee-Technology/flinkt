// SPIKE: hand-written stand-ins for what KSP would generate for these declarations.
package spike.model

import spike.types.GeneratedCodec
import spike.types.GeneratedTypeModule
import spike.types.MutableTypeRegistry

data class UserEvent(val userId: Long, val payload: String)
data class User(val id: Long, val name: String)
data class Session(val userId: Long, val count: Int)

object UserEventCodec : GeneratedCodec<UserEvent> {
    private fun readResolve(): Any = UserEventCodec
    override val typeClass = UserEvent::class.java
    override fun write(value: UserEvent, out: java.io.DataOutput) { out.writeLong(value.userId); out.writeUTF(value.payload) }
    override fun read(input: java.io.DataInput) = UserEvent(input.readLong(), input.readUTF())
}

object UserCodec : GeneratedCodec<User> {
    private fun readResolve(): Any = UserCodec
    override val typeClass = User::class.java
    override fun write(value: User, out: java.io.DataOutput) { out.writeLong(value.id); out.writeUTF(value.name) }
    override fun read(input: java.io.DataInput) = User(input.readLong(), input.readUTF())
}

object SessionCodec : GeneratedCodec<Session> {
    private fun readResolve(): Any = SessionCodec
    override val typeClass = Session::class.java
    override fun write(value: Session, out: java.io.DataOutput) { out.writeLong(value.userId); out.writeInt(value.count) }
    override fun read(input: java.io.DataInput) = Session(input.readLong(), input.readInt())
}

class SpikeModule : GeneratedTypeModule {
    override fun register(registry: MutableTypeRegistry) {
        registry.register(UserEventCodec)
        registry.register(UserCodec)
        registry.register(SessionCodec)
    }
}
