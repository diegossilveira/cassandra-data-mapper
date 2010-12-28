package cassandra.mapper.engine.annotation;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import cassandra.mapper.api.IndexTransformer;
import cassandra.mapper.api.annotation.Index;
import cassandra.mapper.api.exception.CassandraEngineException;
import cassandra.mapper.engine.utils.ReflectionUtils;


public class IndexAnnotationProcessor {

	private final Map<String, IndexInfo> indexInformation;

	public IndexAnnotationProcessor(Class<?> clazz) {

		indexInformation = new HashMap<String, IndexInfo>();
		for (Field field : ReflectionUtils.getAnnotatedFields(clazz, Index.class)) {
			IndexInfo info = new IndexInfo(field, getIndexColumnFamily(field), getIndexTransformer(field));
			indexInformation.put(getIndexName(field), info);
		}
	}

	private String getIndexName(Field field) {

		Index annotation = field.getAnnotation(Index.class);
		return annotation.name();
	}

	private IndexTransformer getIndexTransformer(Field field) {

		try {
			Index annotation = field.getAnnotation(Index.class);
			return annotation.transformer().newInstance();
		} catch (Exception ex) {
			throw new CassandraEngineException(ex);
		}
	}

	private String getIndexColumnFamily(Field field) {

		Index annotation = field.getAnnotation(Index.class);
		return annotation.columnFamily();
	}

	private IndexInfo getIndexInfoForIndexName(String indexName) {

		IndexInfo indexInfo = indexInformation.get(indexName);
		if (indexInfo == null) {
			throw new CassandraEngineException(String.format("No index found for name %s", indexName));
		}
		return indexInfo;
	}

	public String getIndexColumnFamily(String indexName) {

		IndexInfo indexInfo = getIndexInfoForIndexName(indexName);
		return indexInfo.indexColumnFamily();
	}

	public Field getIndexField(String indexName) {
		
		return getIndexInfoForIndexName(indexName).field();
	}
	
	public IndexTransformer getIndexTransformer(String indexName) {
		
		return getIndexInfoForIndexName(indexName).transformer();
	}
	
	public String[] indexNames() {

		return indexInformation.keySet().toArray(new String[0]);
	}

	public int indexCount() {

		return indexInformation.keySet().size();
	}

}
