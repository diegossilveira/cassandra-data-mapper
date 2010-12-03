package cassandra.mapper.engine;

import org.apache.cassandra.thrift.ConsistencyLevel;

import cassandra.mapper.api.annotation.Entity;
import cassandra.mapper.api.exception.CassandraEngineException;


public class EntityAnnotationProcessor {

	private final String columnFamily;
	private final String keyspace;
	private final ConsistencyLevel writeConsistencyLevel;
	private final ConsistencyLevel readConsistencyLevel;
	
	EntityAnnotationProcessor(Class<?> clazz) {

		Entity annotation = clazz.getAnnotation(Entity.class);
		if (annotation == null) {
			throw new CassandraEngineException(String.format("The class '%s' is not a Cassandra @Entity",
					clazz.getCanonicalName()));
		}
		columnFamily = annotation.columnFamily();
		keyspace = annotation.keyspace();
		writeConsistencyLevel = annotation.writeConsistencyLevel();
		readConsistencyLevel = annotation.readConsistencyLevel();
	}
	
	String keyspace() {

		return keyspace;
	}
	
	String columnFamily() {
		
		return columnFamily;
	}

	ConsistencyLevel writeConsistencyLevel() {
		
		return writeConsistencyLevel;
	}

	ConsistencyLevel readConsistencyLevel() {

		return readConsistencyLevel;
	}

}
