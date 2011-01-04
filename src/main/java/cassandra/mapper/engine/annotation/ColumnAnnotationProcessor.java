package cassandra.mapper.engine.annotation;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import cassandra.mapper.api.annotation.Column;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;

public class ColumnAnnotationProcessor {

	private Map<String, Field> columnInformation;

	public ColumnAnnotationProcessor(Class<?> clazz) {

		columnInformation = new HashMap<String, Field>();
		for (Field field : ReflectionUtils.getAnnotatedFields(clazz, Column.class)) {
			columnInformation.put(getColumnName(field), field);
		}
	}

	public String getColumnName(Field field) {

		Column annotation = field.getAnnotation(Column.class);
		return "".equals(annotation.name()) ? field.getName() : annotation.name();
	}

	public Field getColumnField(String columnName) {

		Field field = columnInformation.get(columnName);
		if (field == null) {
			throw new CassandraEngineException(String.format("No column found for name %s", columnName));
		}
		return field;
	}

	public String[] columnNames() {

		return columnInformation.keySet().toArray(new String[0]);
	}

	public int columnCount() {

		return columnInformation.keySet().size();
	}

}
