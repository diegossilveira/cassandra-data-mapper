package cassandra.mapper.engine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.ConsistencyLevel;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.api.FetchMode;
import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.EntityAnnotationProcessor;
import cassandra.mapper.engine.annotation.IndexAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;
import cassandra.mapper.engine.serialization.Deserializer;
import cassandra.mapper.engine.serialization.EagerDeserializer;
import cassandra.mapper.engine.serialization.LazyDeserializer;
import cassandra.mapper.engine.utils.ReflectionUtils;
import cassandra.mapper.engine.utils.TimeUUIDUtils;

public final class EntityProcessor<T> {

	private final Class<T> clazz;

	private final EntityAnnotationProcessor entityProcessor;
	private final ColumnAnnotationProcessor columnProcessor;
	private final IndexAnnotationProcessor indexProcessor;
	private final KeyAnnotationProcessor keyProcessor;

	EntityProcessor(Class<T> clazz) {

		this.clazz = clazz;
		entityProcessor = new EntityAnnotationProcessor(clazz);
		columnProcessor = new ColumnAnnotationProcessor(clazz);
		indexProcessor = new IndexAnnotationProcessor(clazz);
		keyProcessor = new KeyAnnotationProcessor(clazz);
	}

	private Deserializer<T> deserializer(FetchMode fetchMode) {
		
		if (FetchMode.LAZY == fetchMode) {
			return new LazyDeserializer<T>(clazz, columnProcessor, keyProcessor);
		}
		return new EagerDeserializer<T>(clazz, columnProcessor, keyProcessor);
	}

	private byte[] getFieldValueInBytes(T entity, Field field, Transformer transformer) {

		Object value = ReflectionUtils.getFieldValue(field, entity);
		return value == null ? null : transformer.toBytes(value);
		
	}

	public Collection<CassandraColumn> getCassandraColumns(T entity) {

		List<CassandraColumn> columns = new ArrayList<CassandraColumn>();

		for (String columnName : columnProcessor.columnNames()) {

			Field field = columnProcessor.getColumnField(columnName);
			Transformer transformer = columnProcessor.getColumnTransformer(columnName);
			byte[] value = getFieldValueInBytes(entity, field, transformer);
			
			if(value != null) {
				CassandraColumn column = new CassandraColumn(columnName, value);
				columns.add(column);
			}
		}

		return columns;
	}

	public CassandraIndexColumn getCassandraIndexColumn(T entity) {

		UUID indexer = TimeUUIDUtils.getTimeUUID();
		return new CassandraIndexColumn(indexer, getKey(entity));
	}

	public T getCassandraEntity(UUID key, Collection<CassandraColumn> columns) {

		return getCassandraEntity(key, columns, FetchMode.EAGER);
	}

	public T getCassandraEntity(UUID key, Collection<CassandraColumn> columns, FetchMode fetchMode) {

		return deserializer(fetchMode).deserialize(key, columns);
	}

	public UUID getKey(T entity) {

		Object value = ReflectionUtils.getFieldValue(keyProcessor.keyField(), entity);
		if (!(value instanceof UUID)) {
			throw new CassandraEngineException("The key must be an UUID.");
		}
		return (UUID) value;
	}

	public String getIndexKey(T entity, String indexName) {

		Object value = ReflectionUtils.getFieldValue(indexProcessor.getIndexField(indexName), entity);
		return indexProcessor.getIndexTransformer(indexName).toIndexKey(value);
	}

	public String getColumnFamily() {

		return entityProcessor.columnFamily();
	}

	public String getIndexColumnFamily(String indexName) {

		return indexProcessor.getIndexColumnFamily(indexName);
	}

	public String getKeyspace() {

		return entityProcessor.keyspace();
	}

	public ConsistencyLevel getWriteConsistencyLevel() {

		return entityProcessor.writeConsistencyLevel();
	}

	public ConsistencyLevel getReadConsistencyLevel() {

		return entityProcessor.readConsistencyLevel();
	}

	public String[] getColumnNames() {

		return columnProcessor.columnNames();
	}

	public String[] getIndexNames() {

		return indexProcessor.indexNames();
	}

	public int getColumnCount() {

		return columnProcessor.columnCount();
	}

	public int getIndexCount() {

		return indexProcessor.indexCount();
	}

}
