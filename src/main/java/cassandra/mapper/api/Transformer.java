package cassandra.mapper.api;

public interface Transformer {

	byte[] toBytes(Object object);

	Object fromBytes(byte[] bytes);

}
