package cassandra.mapper.engine.serialization;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.UUID;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.Transformer;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;
import cassandra.mapper.engine.annotation.TransformedAnnotationProcessor;
import cassandra.mapper.engine.utils.ReflectionUtils;

public class EagerDeserializer<T> extends AbstractDeserializer<T> {

	public EagerDeserializer(Class<T> clazz, ColumnAnnotationProcessor columnProcessor,
			TransformedAnnotationProcessor transformedProcessor, KeyAnnotationProcessor keyProcessor) {
		super(clazz, columnProcessor, transformedProcessor, keyProcessor);
	}

	@Override
	public T deserialize(UUID key, Collection<CassandraColumn> columns) {

		T entity = ReflectionUtils.instantiate(clazz);
		ReflectionUtils.setFieldValue(keyProcessor.keyField(), entity, key);

		for (CassandraColumn column : columns) {

			Field columnField = columnProcessor.getColumnField(column.name());
			Transformer transformer = transformedProcessor.getColumnTransformer(columnField);
			setFieldValueFromBytes(entity, columnField, column.value(), transformer);
		}

		return entity;
	}

}
