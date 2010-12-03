package cassandra.mapper.api;

import java.util.List;
import java.util.UUID;

public interface IndexedMapper extends Mapper {

	<E> void updateIndex(E entity, String indexName);
	
	<E> void removeIndexEntry(Class<E> clazz, String indexName, String indexKey);
	
	<E> void removeIndexedColumn(Class<E> clazz, String indexName, String indexKey, CassandraIndexColumn column);
	
	<E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey);
	
	<E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey, UUID initialIndexer, int size);
	
}
