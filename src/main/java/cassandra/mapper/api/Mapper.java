package cassandra.mapper.api;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import cassandra.mapper.api.exception.CassandraMapperException;


public interface Mapper {

	/**
	 * Stores an entity in Cassandra
	 * 
	 * @param <E>
	 * @param entity
	 * @throws CassandraMapperException
	 */
	<E> UUID store(E entity);

	/**
	 * Finds the entity with given key in Cassandra
	 * 
	 * @param <E>
	 * @param key
	 * @param clazz
	 * @return
	 * @throws CassandraMapperException
	 */
	<E> E findByKey(UUID key, Class<E> clazz);

	/**
	 * Finds all entities with given keys in Cassandra
	 * 
	 * @param <E>
	 * @param keys
	 * @param clazz
	 * @return
	 * @throws CassandraMapperException
	 */
	<E> List<E> findByKeys(Collection<UUID> keys, Class<E> clazz);

	/**
	 * Finds a range of contiguous keys in Cassandra, starting from a given initialKey. With null initialKey, the range
	 * starts from the beginning. The order is not guaranteed, unless using OrderPreservingPartitioner.
	 * 
	 * @param <E>
	 * @param initialKey
	 * @param clazz
	 * @param size
	 * @return
	 * @throws CassandraMapperException
	 */
	<E> CassandraRange<E> findByRange(UUID initialKey, Class<E> clazz, int size);

	/**
	 * Removes an entity from Cassandra. Depending on implementation, the Row associated with the entity's key may
	 * remain in Cassandra, with no columns.
	 * 
	 * @param <E>
	 * @param entity
	 * @throws CassandraMapperException
	 */
	<E> void remove(E entity);

	/**
	 * Removes an entity from Cassandra, with the given key. Depending on implementation, the Row associated with the
	 * entity's key may remain in Cassandra, with no columns.
	 * 
	 * @param <E>
	 * @param key
	 * @param clazz
	 * @throws CassandraMapperException
	 */
	<E> void remove(UUID key, Class<E> clazz);

}
