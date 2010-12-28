package cassandra.mapper.engine.serialization;

import cassandra.mapper.api.Transformer;

public class LazyColumnInfo {

	private Transformer transformer;
	private byte[] bytes;
	private Object value;

	public LazyColumnInfo(Transformer transformer, byte[] bytes) {

		this.transformer = transformer;
		this.bytes = bytes;
	}

	public LazyColumnInfo(Object value) {
		
		this.value = value;
	}

	public Object value() {
		
		if(value == null) {
			value = transformer.fromBytes(bytes);
		}
		return value;
	}

}
