package cassandra.mapper.engine;

import java.lang.reflect.Field;

import cassandra.mapper.api.annotation.Key;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;


public class KeyAnnotationProcessor {

	private final Field keyField;

	KeyAnnotationProcessor(Class<?> clazz) {

		keyField = ReflectionUtils.getFirstAnnotatedField(clazz, Key.class);
		if (keyField == null) {
			throw new CassandraEngineException(String.format("No @Key field found in class '%s'",
					clazz.getCanonicalName()));
		}
	}

	Field keyField() {
		return keyField;
	}

}
