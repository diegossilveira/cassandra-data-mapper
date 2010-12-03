package cassandra.mapper.engine;

import java.lang.reflect.Field;

import cassandra.mapper.api.Transformer;


class ColumnInfo {

	private final Field field;
	private final Transformer transformer;

	ColumnInfo(Field field, Transformer transformer) {
		
		this.field = field;
		this.transformer = transformer;
	}

	Field field() {
		
		return field;
	}

	Transformer transformer() {
		
		return transformer;
	}

}
