package cassandra.mapper.hector;

import java.util.UUID;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

public class UUIDDeserializer extends AbstractSerializer<UUID> {

	@Override
	public UUID fromBytes(byte[] bytes) {
		return UUID.nameUUIDFromBytes(bytes);
	}

	@Override
	public byte[] toBytes(UUID uuid) {
		throw new UnsupportedOperationException("Operation not supported");
	}

}
