package cassandra.mapper.engine.serialization;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import net.sf.cglib.proxy.Enhancer;
import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.Transformer;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;

public class LazyDeserializer<T> extends AbstractDeserializer<T> {

	public LazyDeserializer(Class<T> clazz, ColumnAnnotationProcessor columnProcessor, KeyAnnotationProcessor keyProcessor) {
		super(clazz, columnProcessor, keyProcessor);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(UUID key, Collection<CassandraColumn> columns) {

		Enhancer enhacer = new Enhancer();
		enhacer.setSuperclass(clazz);
		enhacer.setCallback(new LazyObjectHandler(clazz, prepareColumnMap(key, columns)));
		
		return (T) enhacer.create();
		
	}

	private Map<Field, Object> prepareColumnMap(UUID key, Collection<CassandraColumn> columns) {

		Map<Field, Object> columnMap = new HashMap<Field, Object>();
		
		columnMap.put(keyProcessor.keyField(), key);
		
		for (CassandraColumn column : columns) {
			
			Field columnField = columnProcessor.getColumnField(column.name());
			Transformer transformer = columnProcessor.getColumnTransformer(column.name());
			columnMap.put(columnField, transformer.fromBytes(column.value()));
		}
		
		return columnMap;
	}

}