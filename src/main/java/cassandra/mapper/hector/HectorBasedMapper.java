package cassandra.mapper.hector;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
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
import cassandra.mapper.api.CassandraRange;
import cassandra.mapper.api.FetchMode;
import cassandra.mapper.api.Mapper;
import cassandra.mapper.api.exception.CassandraMapperException;
import cassandra.mapper.engine.EntityProcessor;

public class HectorBasedMapper extends AbstractHectorMapper implements Mapper {

	private final Logger logger = Logger.getLogger(HectorBasedMapper.class);

	public HectorBasedMapper(CassandraCluster cassandraCluster) {
		super(cassandraCluster);
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
				
				if(column.value() != null) {
					
					HColumn<String, byte[]> hColumn = HFactory.createColumn(column.name(), column.value(), stringSerializer(), byteSerializer());
					mutator.addInsertion(key.toString(), processor.getColumnFamily(), hColumn);
					
				} else {
				
					mutator.addDeletion(key.toString(), processor.getColumnFamily(), column.name(), stringSerializer());
				}
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
			SliceQuery<String, byte[]> query = HFactory.createSliceQuery(keyspace(clazz), stringSerializer(), byteSerializer());
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
			MultigetSliceQuery<String, byte[]> query = HFactory.createMultigetSliceQuery(keyspace(clazz), stringSerializer(),
					byteSerializer());
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
			RangeSlicesQuery<String, byte[]> query = HFactory.createRangeSlicesQuery(keyspace(clazz), stringSerializer(), byteSerializer());
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

			mutator.delete(key.toString(), processor.getColumnFamily(), null, stringSerializer());

		} catch (Exception ex) {
			throw new CassandraMapperException("Error while removing entity", ex);
		}

		logger.debug("Successfully removed entity with key " + key);
		logger.info("The key " + key + " (with no columns) will still be shown in full range searches");
	}

}
