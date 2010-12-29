package cassandra.mapper.engine.serialization;

import java.util.Collection;
import java.util.UUID;

import net.sf.cglib.proxy.Enhancer;
import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.engine.annotation.ColumnAnnotationProcessor;
import cassandra.mapper.engine.annotation.KeyAnnotationProcessor;

public class LazyDeserializer<T> extends AbstractDeserializer<T> {

	public LazyDeserializer(Class<T> clazz, ColumnAnnotationProcessor columnProcessor, KeyAnnotationProcessor keyProcessor) {
		super(clazz, columnProcessor, keyProcessor);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(UUID key, Collection<CassandraColumn> columns) {

		LazyObjectHandler objectHandler = LazyObjectHandler.BUILDER.forClass(clazz).key(key).onColumns(columns).with(columnProcessor)
				.with(keyProcessor).build();

		Enhancer enhacer = new Enhancer();
		enhacer.setSuperclass(clazz);
		enhacer.setCallback(objectHandler);

		return (T) enhacer.create();
	}

}
