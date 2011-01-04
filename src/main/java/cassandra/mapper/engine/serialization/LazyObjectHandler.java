package cassandra.mapper.engine.serialization;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.log4j.Logger;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.Transformer;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;
import cassandra.mapper.engine.annotation.TransformedAnnotationProcessor;
import cassandra.mapper.engine.utils.ReflectionUtils;

public class LazyObjectHandler implements MethodInterceptor {

	private final Logger logger = Logger.getLogger(LazyDeserializer.class);
	private final Class<?> clazz;
	private final UUID key;
	private final ColumnAnnotationProcessor columnProcessor;
	private final TransformedAnnotationProcessor transformedProcessor;
	private final KeyAnnotationProcessor keyProcessor;
	private final List<CassandraColumn> columns;
	
	public static final Builder BUILDER = new Builder();

	public LazyObjectHandler(Builder builder) {
		this.clazz = builder.clazz;
		this.key = builder.key;
		this.columnProcessor = builder.columnProcessor;
		this.transformedProcessor = builder.transformedProcessor;
		this.keyProcessor = builder.keyProcessor;
		this.columns = new ArrayList<CassandraColumn>(builder.columns);
	}

	@Override
	public Object intercept(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {

		logger.debug("Proxy: invoked method " + method.getName());

		// Probably not a get method 
		if (Void.TYPE == method.getGenericReturnType()) {

			return methodProxy.invokeSuper(object, args);
		}

		Field field = ReflectionUtils.getFieldForMethod(clazz, method);

		// No field found for this method name
		if (field == null) {

			return methodProxy.invokeSuper(object, args);
		}
		
		Object value = ReflectionUtils.getFieldValue(field, object);
		
		// The value is already set
		if(value != null) {
			
			return value;
		}
		
		value = getValue(object, field);

		// Can't find a value for this field on Cassandra
		if(value == null) {
			
			return methodProxy.invokeSuper(object, args);
		}
		
		ReflectionUtils.setFieldValue(field, object, value);

		return value;
	}
	
	private Object getValue(Object object, Field field) {
		
		Object keyValue = getKeyValue(field);
		if(keyValue != null) {
			return keyValue;
		}
		
		String columnName = columnProcessor.getColumnName(field);
		CassandraColumn column = new CassandraColumn(columnName, null);
		int columnIndex = columns.indexOf(column);
		if(columnIndex < 0) {
			return null;
		}
		
		column = columns.get(columnIndex);
		Transformer transformer = transformedProcessor.getColumnTransformer(field);
		return transformer.fromBytes(column.value());
	}
	
	private Object getKeyValue(Field field) {
		
		return keyProcessor.keyField().equals(field) ? key : null;
	}

	public static class Builder {

		private Class<?> clazz;
		private UUID key;
		private ColumnAnnotationProcessor columnProcessor;
		private TransformedAnnotationProcessor transformedProcessor;
		private KeyAnnotationProcessor keyProcessor;
		private Collection<CassandraColumn> columns;

		private Builder() {

		}

		public Builder forClass(Class<?> clazz) {
			this.clazz = clazz;
			return this;
		}
		
		public Builder key(UUID key) {
			this.key = key;
			return this;
		}

		public Builder with(ColumnAnnotationProcessor columnProcessor) {
			this.columnProcessor = columnProcessor;
			return this;
		}
		
		public Builder with(TransformedAnnotationProcessor transformedProcessor) {
			this.transformedProcessor = transformedProcessor;
			return this;
		}

		public Builder with(KeyAnnotationProcessor keyProcessor) {
			this.keyProcessor = keyProcessor;
			return this;
		}

		public Builder onColumns(Collection<CassandraColumn> columns) {
			this.columns = columns;
			return this;
		}

		public LazyObjectHandler build() {
			return new LazyObjectHandler(this);
		}
	}
}
