package cassandra.mapper.engine;

import java.lang.reflect.Field;

import cassandra.mapper.api.IndexTransformer;


class IndexInfo {

	private final Field field;
	private final String indexColumnFamily;
	private final IndexTransformer transformer;

	IndexInfo(Field field, String indexColumnFamily, IndexTransformer transformer) {
		
		this.field = field;
		this.indexColumnFamily = indexColumnFamily;
		this.transformer = transformer;
	}

	Field field() {
		
		return field;
	}

	IndexTransformer transformer() {
		
		return transformer;
	}

	String indexColumnFamily() {
		
		return indexColumnFamily;
	}

}
