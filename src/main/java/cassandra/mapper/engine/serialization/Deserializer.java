package cassandra.mapper.engine.serialization;

import java.util.Collection;
import java.util.UUID;

import cassandra.mapper.api.CassandraColumn;

public interface Deserializer<T> {

	T deserialize(UUID key, Collection<CassandraColumn> columns);
	
}
