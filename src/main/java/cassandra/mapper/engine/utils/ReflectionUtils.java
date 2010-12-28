package cassandra.mapper.engine.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import cassandra.mapper.api.exception.CassandraEngineException;

public abstract class ReflectionUtils {

	public static Field[] getAnnotatedFields(Class<?> clazz, Class<? extends Annotation> annotation) {

		List<Field> fields = new ArrayList<Field>();

		Field[] allFields = clazz.getDeclaredFields();
		for (Field field : allFields) {
			if (field.isAnnotationPresent(annotation)) {
				fields.add(field);
			}
		}

		return fields.toArray(new Field[0]);
	}

	public static Field getFirstAnnotatedField(Class<?> clazz, Class<? extends Annotation> annotation) {

		Field[] fields = getAnnotatedFields(clazz, annotation);
		return fields.length > 0 ? fields[0] : null;
	}

	public static <T> void setFieldValue(Field field, T object, Object value) {

		try {
			field.setAccessible(true);
			field.set(object, value);
		} catch (Exception ex) {
			throw new CassandraEngineException(ex);
		}
	}

	public static <T> Object getFieldValue(Field field, T object) {

		try {
			field.setAccessible(true);
			return field.get(object);
		} catch (Exception ex) {
			throw new CassandraEngineException(String.format("Can't get the value (%s) for class '%s'", field.getName(), object.getClass()
					.getCanonicalName()), ex);
		}
	}

	public static <T> T instantiate(Class<T> type) {

		try {
			// TODO: change the way the entity is instantiated, without the need for a default constructor
			return type.getConstructor().newInstance();
		} catch (Exception ex) {
			throw new CassandraEngineException(String.format("Can't instantiate an object of type '%s'", type.getCanonicalName()), ex);
		}
	}

	public static <T> Field getFieldForMethod(Class<T> type, Method method) {

		try {

			return type.getDeclaredField(extractFieldName(method));
		} catch (NoSuchFieldException ex) {
			return null;
		}
	}

	private static String extractFieldName(Method method) {

		String fieldName = method.getName();

		String[] knownPrefixes = new String[] { "is", "get", "set" };

		for (String prefix : knownPrefixes) {

			if (fieldName.startsWith(prefix)) {
				fieldName = fieldName.replaceFirst(prefix, "");
			}
		}

		return fieldName.toLowerCase();
	}
}
