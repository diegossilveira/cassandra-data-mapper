package cassandra.mapper.hector;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import cassandra.mapper.api.CassandraCluster;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.api.CassandraRange;
import cassandra.mapper.api.FetchMode;
import cassandra.mapper.api.IndexedMapper;
import cassandra.mapper.api.exception.CassandraMapperException;
import cassandra.mapper.engine.EntityProcessor;

public class HectorBasedIndexedMapper extends AbstractHectorMapper implements IndexedMapper {

	private final Logger logger = Logger.getLogger(HectorBasedIndexedMapper.class);
	private final HectorBasedMapper mapper;

	public HectorBasedIndexedMapper(CassandraCluster cassandraCluster) {
		super(cassandraCluster);
		mapper = new HectorBasedMapper(cassandraCluster);
	}

	@Override
	public <E> UUID store(E entity) {
		return mapper.store(entity);
	}

	@Override
	public <E> E findByKey(UUID key, Class<E> clazz) {
		return mapper.findByKey(key, clazz);
	}

	@Override
	public <E> E findByKey(UUID key, Class<E> clazz, FetchMode fetchMode) {
		return mapper.findByKey(key, clazz, fetchMode);
	}

	@Override
	public <E> List<E> findByKeys(Collection<UUID> keys, Class<E> clazz) {
		return mapper.findByKeys(keys, clazz);
	}

	@Override
	public <E> List<E> findByKeys(Collection<UUID> keys, Class<E> clazz, FetchMode fetchMode) {
		return mapper.findByKeys(keys, clazz, fetchMode);
	}

	@Override
	public <E> CassandraRange<E> findByRange(UUID initialKey, Class<E> clazz, int size) {
		return mapper.findByRange(initialKey, clazz, size);
	}

	@Override
	public <E> CassandraRange<E> findByRange(UUID initialKey, Class<E> clazz, int size, FetchMode fetchMode) {
		return mapper.findByRange(initialKey, clazz, size, fetchMode);
	}

	@Override
	public <E> void remove(E entity) {
		mapper.remove(entity);

	}

	@Override
	public <E> void remove(UUID key, Class<E> clazz) {
		mapper.remove(key, clazz);
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
					HFactory.createColumn(column.indexer(), column.indexedKey(), uuidSerializer(), uuidSerializer()));
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

			mutator.delete(indexKey, processor.getIndexColumnFamily(indexName), columnIndexer, uuidSerializer());

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
			SliceQuery<UUID, UUID> query = HFactory.createSliceQuery(keyspace(clazz), uuidSerializer(), uuidSerializer());
			query.setColumnFamily(processor.getIndexColumnFamily(indexName));
			query.setRange(initialIndexer, null, false, size);
			query.setKey(indexKey);

			QueryResult<ColumnSlice<UUID, UUID>> result = query.execute();

			return HectorCommons.buildIndexes(result.get());

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while retrieving key", ex);
		}
	}

}
