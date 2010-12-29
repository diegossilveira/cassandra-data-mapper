package cassandra.mapper.hector;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import cassandra.mapper.api.CassandraCluster;
import cassandra.mapper.engine.EntityProcessor;
import cassandra.mapper.engine.EntityProcessorFactory;

public abstract class AbstractHectorMapper {

	private final Cluster cluster;

	// Hector Serializers
	private final StringSerializer stringSerializer;
	private final BytesSerializer byteSerializer;
	private final UUIDSerializer uuidSerializer;

	// Constants
	protected static final String EMPTY_COLUMN = "";
	protected static final String EMPTY_KEY = "";

	protected AbstractHectorMapper(CassandraCluster cassandraCluster) {

		CassandraHostConfigurator configurator = new HectorConfigurationManager(cassandraCluster).configurator();
		cluster = HFactory.getOrCreateCluster(cassandraCluster.name(), configurator);
		stringSerializer = new StringSerializer();
		byteSerializer = new BytesSerializer();
		uuidSerializer = new UUIDSerializer();
	}

	protected <E> ConsistencyLevelPolicy consistencyLevel(Class<E> clazz) {

		EntityProcessor<E> processor = processor(clazz);
		ConfigurableConsistencyLevel consistencyLevel = new ConfigurableConsistencyLevel();
		consistencyLevel.setDefaultReadConsistencyLevel(processor.getReadConsistencyLevel());
		consistencyLevel.setDefaultWriteConsistencyLevel(processor.getWriteConsistencyLevel());

		return consistencyLevel;
	}

	protected <E> EntityProcessor<E> processor(Class<E> clazz) {

		return EntityProcessorFactory.getEntityProcessor(clazz);
	}

	protected <E> Keyspace keyspace(Class<E> clazz) {

		return HFactory.createKeyspace(processor(clazz).getKeyspace(), cluster, consistencyLevel(clazz));
	}

	protected StringSerializer stringSerializer() {
		return stringSerializer;
	}

	protected BytesSerializer byteSerializer() {
		return byteSerializer;
	}

	protected UUIDSerializer uuidSerializer() {
		return uuidSerializer;
	}

}
