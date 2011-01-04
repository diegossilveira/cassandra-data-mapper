package cassandra.mapper.engine.serialization;

import java.lang.reflect.Field;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;
import cassandra.mapper.engine.annotation.TransformedAnnotationProcessor;
import cassandra.mapper.engine.utils.ReflectionUtils;

public abstract class AbstractDeserializer<T> implements Deserializer<T> {

	protected final Class<T> clazz;

	protected final ColumnAnnotationProcessor columnProcessor;
	protected final TransformedAnnotationProcessor transformedProcessor;
	protected final KeyAnnotationProcessor keyProcessor;

	protected AbstractDeserializer(Class<T> clazz, ColumnAnnotationProcessor columnProcessor,
			TransformedAnnotationProcessor transformedProcessor, KeyAnnotationProcessor keyProcessor) {

		this.clazz = clazz;
		this.columnProcessor = columnProcessor;
		this.transformedProcessor = transformedProcessor;
		this.keyProcessor = keyProcessor;
	}

	protected void setFieldValueFromBytes(T entity, Field field, byte[] bytes, Transformer transformer) {

		Object value = transformer.fromBytes(bytes);
		ReflectionUtils.setFieldValue(field, entity, value);
	}

}
