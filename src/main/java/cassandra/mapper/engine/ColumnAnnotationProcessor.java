package cassandra.mapper.engine;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.annotation.Column;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;


public class ColumnAnnotationProcessor {

	private Map<String, ColumnInfo> columnInformation;

	ColumnAnnotationProcessor(Class<?> clazz) {

		columnInformation = new HashMap<String, ColumnInfo>();
		for (Field field : ReflectionUtils.getAnnotatedFields(clazz, Column.class)) {
			ColumnInfo info = new ColumnInfo(field, getColumnTransformer(field));
			columnInformation.put(getColumnName(field), info);
		}
	}

	private String getColumnName(Field field) {

		Column annotation = field.getAnnotation(Column.class);
		return "".equals(annotation.name()) ? field.getName() : annotation.name();
	}

	private Transformer getColumnTransformer(Field field) {

		try {
			Column annotation = field.getAnnotation(Column.class);
			return annotation.transformer().newInstance();
		} catch (Exception ex) {
			throw new CassandraEngineException(ex);
		}
	}
	
	private ColumnInfo getColumnInfoForColumnName(String columnName) {

		ColumnInfo columnInfo = columnInformation.get(columnName);
		if (columnInfo == null) {
			throw new CassandraEngineException(String.format("No column found for name %s", columnName));
		}
		return columnInfo;
	}

	Field getColumnField(String columnName) {

		return getColumnInfoForColumnName(columnName).field();
	}

	Transformer getColumnTransformer(String columnName) {

		return getColumnInfoForColumnName(columnName).transformer();
	}

	String[] columnNames() {

		return columnInformation.keySet().toArray(new String[0]);
	}

	int columnCount() {

		return columnInformation.keySet().size();
	}

}
