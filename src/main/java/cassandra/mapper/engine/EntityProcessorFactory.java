package cassandra.mapper.engine;

import java.util.HashMap;
import java.util.Map;

//TODO: improve cache management, this is an extremely basic one, using a raw Map
public abstract class EntityProcessorFactory {

	private static final Map<Class<?>, EntityProcessor<?>> cache = new HashMap<Class<?>, EntityProcessor<?>>();

	public static <T> EntityProcessor<T> getEntityProcessor(Class<T> clazz) {

		@SuppressWarnings("unchecked")
		EntityProcessor<T> processor = (EntityProcessor<T>) cache.get(clazz);
		if (processor == null) {
			processor = new EntityProcessor<T>(clazz);
			cache.put(clazz, processor);
		}
		return processor;
	}

}
