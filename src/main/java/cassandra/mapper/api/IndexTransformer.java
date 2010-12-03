package cassandra.mapper.api;

public interface IndexTransformer {

	String toIndexKey(Object object);

}
