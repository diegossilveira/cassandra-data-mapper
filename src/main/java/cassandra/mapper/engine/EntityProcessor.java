package cassandra.mapper.engine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.ConsistencyLevel;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.CassandraEngineException;
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

	private byte[] getFieldValueInBytes(T entity, Field field, Transformer transformer) {

		Object value = ReflectionUtils.getFieldValue(field, entity);
		return transformer.toBytes(value);
	}

	private void setFieldValueFromBytes(T entity, Field field, byte[] bytes, Transformer transformer) {

		Object value = transformer.fromBytes(bytes);
		ReflectionUtils.setFieldValue(field, entity, value);
	}

	public Collection<CassandraColumn> getCassandraColumns(T entity) {

		List<CassandraColumn> columns = new ArrayList<CassandraColumn>();

		for (String columnName : columnProcessor.columnNames()) {

			Field field = columnProcessor.getColumnField(columnName);
			Transformer transformer = columnProcessor.getColumnTransformer(columnName);
			CassandraColumn column = new CassandraColumn(columnName, getFieldValueInBytes(entity, field, transformer));
			columns.add(column);
		}

		return columns;
	}

	public CassandraIndexColumn getCassandraIndexColumn(T entity) {

		UUID indexer = TimeUUIDUtils.getTimeUUID();
		return new CassandraIndexColumn(indexer, getKey(entity));
	}

	public T getCassandraEntity(UUID key, Collection<CassandraColumn> columns) {

		T entity = ReflectionUtils.instantiate(clazz);
		ReflectionUtils.setFieldValue(keyProcessor.keyField(), entity, key);

		for (CassandraColumn column : columns) {

			Field columnField = columnProcessor.getColumnField(column.name());
			Transformer transformer = columnProcessor.getColumnTransformer(column.name());
			setFieldValueFromBytes(entity, columnField, column.value(), transformer);
		}

		return entity;
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