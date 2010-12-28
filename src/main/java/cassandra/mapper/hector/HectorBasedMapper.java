package cassandra.mapper.hector;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;

import cassandra.mapper.api.CassandraCluster;
import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.api.CassandraRange;
import cassandra.mapper.api.FetchMode;
import cassandra.mapper.api.IndexedMapper;
import cassandra.mapper.api.exception.CassandraMapperException;
import cassandra.mapper.engine.EntityProcessor;
import cassandra.mapper.engine.EntityProcessorFactory;

public class HectorBasedMapper implements IndexedMapper {

	private final Cluster cluster;
	private final StringSerializer stringSerializer;
	private final BytesSerializer byteSerializer;
	private final UUIDSerializer uuidSerializer;
	private static final String EMPTY_COLUMN = "";
	private static final String EMPTY_KEY = "";
	private final Logger logger = Logger.getLogger(HectorBasedMapper.class);

	public HectorBasedMapper(CassandraCluster cassandraCluster) {

		CassandraHostConfigurator configurator = new HectorConfigurationManager(cassandraCluster).configurator();
		cluster = HFactory.getOrCreateCluster(cassandraCluster.name(), configurator);
		stringSerializer = new StringSerializer();
		byteSerializer = new BytesSerializer();
		uuidSerializer = new UUIDSerializer();
	}

	@Override
	public <E> UUID store(E entity) {

		logger.debug("Storing entity");

		try {
			@SuppressWarnings("unchecked")
			EntityProcessor<E> processor = processor((Class<E>) entity.getClass());
			Mutator mutator = HFactory.createMutator(keyspace(entity.getClass()));
			Collection<CassandraColumn> columns = processor.getCassandraColumns(entity);

			UUID key = processor.getKey(entity);
			logger.info("Storing with id " + key.toString());

			for (CassandraColumn column : columns) {
				mutator.addInsertion(key.toString(), processor.getColumnFamily(),
						HFactory.createColumn(column.name(), column.value(), stringSerializer, byteSerializer));
			}
			mutator.execute();

			logger.debug("Successfuly stored entity");

			return key;

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while storing entity", ex);
		}
	}

	@Override
	public <E> E findByKey(UUID key, Class<E> clazz) {

		return findByKey(key, clazz, FetchMode.EAGER);
	}

	@Override
	public <E> E findByKey(UUID key, Class<E> clazz, FetchMode fetchMode) {

		logger.debug("Finding entity with key " + key);

		try {

			EntityProcessor<E> processor = processor(clazz);
			SliceQuery<String, byte[]> query = HFactory.createSliceQuery(keyspace(clazz), stringSerializer, byteSerializer);
			query.setColumnFamily(processor.getColumnFamily()).setKey(key.toString()).setColumnNames(processor.getColumnNames());

			QueryResult<ColumnSlice<String, byte[]>> result = query.execute();

			return HectorCommons.buildEntity(result.get(), key, processor, fetchMode);

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while retrieving key", ex);
		}
	}

	@Override
	public <E> List<E> findByKeys(Collection<UUID> keys, Class<E> clazz) {

		return findByKeys(keys, clazz, FetchMode.EAGER);
	}

	@Override
	public <E> List<E> findByKeys(Collection<UUID> keys, Class<E> clazz, FetchMode fetchMode) {

		logger.debug("Finding entities with keys " + keys);

		try {

			EntityProcessor<E> processor = processor(clazz);
			MultigetSliceQuery<String, byte[]> query = HFactory.createMultigetSliceQuery(keyspace(clazz), stringSerializer, byteSerializer);
			query.setColumnFamily(processor.getColumnFamily());
			query.setKeys(HectorCommons.toStringKeys(keys));
			query.setRange(EMPTY_COLUMN, EMPTY_COLUMN, false, processor.getColumnCount());

			QueryResult<Rows<String, byte[]>> result = query.execute();

			return HectorCommons.buildEntities(result.get(), null, processor, fetchMode);

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while retrieving keys", ex);
		}
	}

	@Override
	public <E> CassandraRange<E> findByRange(UUID initialKey, Class<E> clazz, int size) {

		return findByRange(initialKey, clazz, size, FetchMode.EAGER);
	}

	@Override
	public <E> CassandraRange<E> findByRange(UUID initialKey, Class<E> clazz, int size, FetchMode fetchMode) {

		logger.debug("Finding entities within range starting at key " + initialKey);

		try {

			EntityProcessor<E> processor = processor(clazz);
			RangeSlicesQuery<String, byte[]> query = HFactory.createRangeSlicesQuery(keyspace(clazz), stringSerializer, byteSerializer);
			query.setColumnFamily(processor.getColumnFamily());
			query.setKeys(initialKey == null ? "" : initialKey.toString(), EMPTY_KEY);
			query.setRange(EMPTY_COLUMN, EMPTY_COLUMN, false, processor.getColumnCount());
			query.setRowCount(size + 1);

			QueryResult<OrderedRows<String, byte[]>> result = query.execute();
			OrderedRows<String, byte[]> rows = result.get();

			UUID lastKey = null;
			if (rows.getCount() > size) {
				Row<String, byte[]> lastRow = HectorCommons.getLastRow(rows);
				lastKey = UUID.fromString(lastRow.getKey());
				logger.debug("Next key is " + lastKey);
			}

			List<E> list = HectorCommons.buildEntities(rows, lastKey, processor, fetchMode);

			return new CassandraRange<E>(list, lastKey);

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while retrieving range", ex);
		}
	}

	@Override
	public <E> void remove(E entity) {

		@SuppressWarnings("unchecked")
		EntityProcessor<E> processor = processor((Class<E>) entity.getClass());
		remove(processor.getKey(entity), entity.getClass());
	}

	@Override
	public <E> void remove(UUID key, Class<E> clazz) {

		logger.debug("Removing entity with key " + key);

		try {

			EntityProcessor<E> processor = processor(clazz);
			Mutator mutator = HFactory.createMutator(keyspace(clazz));

			mutator.delete(key.toString(), processor.getColumnFamily(), null, stringSerializer);

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while removing entity", ex);
		}

		logger.debug("Successfully removed entity with key " + key);
		logger.info("The key " + key + " (with no columns) will still be shown in full range searches");
	}

	@Override
	public <E> void updateIndex(E entity, String indexName) {

		logger.debug("Storing index");

		try {
			@SuppressWarnings("unchecked")
			EntityProcessor<E> processor = processor((Class<E>) entity.getClass());
			Mutator mutator = HFactory.createMutator(keyspace(entity.getClass()));
			CassandraIndexColumn column = processor.getCassandraIndexColumn(entity);

			String key = processor.getIndexKey(entity, indexName);

			mutator.addInsertion(key, processor.getIndexColumnFamily(indexName),
					HFactory.createColumn(column.indexer(), column.indexedKey(), uuidSerializer, uuidSerializer));
			mutator.execute();

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while storing entity", ex);
		}

		logger.debug("Successfuly stored index");
	}

	@Override
	public <E> void removeIndexEntry(Class<E> clazz, String indexName, String indexKey) {

		logger.debug("Removing indexed keys under key " + indexKey);

		removeIndexedColumn(clazz, indexName, indexKey, null);
	}

	@Override
	public <E> void removeIndexedColumn(Class<E> clazz, String indexName, String indexKey, CassandraIndexColumn column) {

		logger.debug("Removing indexed key " + column.indexedKey());

		try {

			EntityProcessor<E> processor = processor(clazz);
			Mutator mutator = HFactory.createMutator(keyspace(clazz));

			UUID columnIndexer = (column == null) ? null : column.indexer();

			mutator.delete(indexKey, processor.getIndexColumnFamily(indexName), columnIndexer, uuidSerializer);

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while removing entity", ex);
		}

		logger.debug("Successfully removed indexed keys under key " + indexKey);
	}

	@Override
	public <E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey) {

		return findIndexedColumns(clazz, indexName, indexKey, null, Integer.MAX_VALUE);
	}

	@Override
	public <E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey, UUID initialIndexer,
			int size) {

		logger.debug("Finding indexed keys");

		try {

			EntityProcessor<E> processor = processor(clazz);
			SliceQuery<UUID, UUID> query = HFactory.createSliceQuery(keyspace(clazz), uuidSerializer, uuidSerializer);
			query.setColumnFamily(processor.getIndexColumnFamily(indexName));
			query.setRange(initialIndexer, null, false, size);
			query.setKey(indexKey);

			QueryResult<ColumnSlice<UUID, UUID>> result = query.execute();

			return HectorCommons.buildIndexes(result.get());

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while retrieving key", ex);
		}
	}

	private <E> ConsistencyLevelPolicy consistencyLevel(Class<E> clazz) {

		EntityProcessor<E> processor = processor(clazz);
		ConfigurableConsistencyLevel consistencyLevel = new ConfigurableConsistencyLevel();
		consistencyLevel.setDefaultReadConsistencyLevel(processor.getReadConsistencyLevel());
		consistencyLevel.setDefaultWriteConsistencyLevel(processor.getWriteConsistencyLevel());

		return consistencyLevel;
	}

	private <E> EntityProcessor<E> processor(Class<E> clazz) {

		return EntityProcessorFactory.getEntityProcessor(clazz);
	}

	private <E> Keyspace keyspace(Class<E> clazz) {

		return HFactory.createKeyspace(processor(clazz).getKeyspace(), cluster, consistencyLevel(clazz));
	}

}
