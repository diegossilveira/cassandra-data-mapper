package cassandra.mapper.api;

import java.util.List;
import java.util.UUID;

public interface IndexedMapper extends Mapper {

	/**
	 * Updates the index mapped on entity with given indexName.
	 * 
	 * @param <E>
	 * @param entity
	 * @param indexName
	 */
	<E> void updateIndex(E entity, String indexName);
	
	/**
	 * Removes an entry and all columns mapped by the index mapped on entity with given indexName.
	 * 
	 * @param <E>
	 * @param clazz
	 * @param indexName
	 * @param indexKey
	 */
	<E> void removeIndexEntry(Class<E> clazz, String indexName, String indexKey);
	
	/**
	 * Removes a single column indexed by the index mapped on entity with given indexName.
	 * 
	 * @param <E>
	 * @param clazz
	 * @param indexName
	 * @param indexKey
	 * @param column
	 */
	<E> void removeIndexedColumn(Class<E> clazz, String indexName, String indexKey, CassandraIndexColumn column);
	
	/**
	 * Retrieves all columns indexed by the index mapped on entity with given indexName. 
	 * 
	 * @param <E>
	 * @param clazz
	 * @param indexName
	 * @param indexKey
	 * @return
	 */
	<E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey);
	
	/**
	 * Retrieves some (size) columns indexed by the index mapped on entity with given indexName. 
	 * 
	 * @param <E>
	 * @param clazz
	 * @param indexName
	 * @param indexKey
	 * @param initialIndexer
	 * @param size
	 * @return
	 */
	<E> List<CassandraIndexColumn> findIndexedColumns(Class<E> clazz, String indexName, String indexKey, UUID initialIndexer, int size);
	
}
