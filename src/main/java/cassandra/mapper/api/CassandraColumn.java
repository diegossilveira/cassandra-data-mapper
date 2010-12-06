package cassandra.mapper.api;

import java.util.Arrays;

public class CassandraColumn {

	private final String name;

	private final byte[] value;
	
	private long timestamp;

	public CassandraColumn(String name, Object value, Transformer valueTransformer) {
		this.name = name;
		this.value = valueTransformer.toBytes(value);
	}
	
	public CassandraColumn(String name, byte[] value) {
		this.name = name;
		this.value = value;
	}

	public String name() {
		return name;
	}

	public byte[] value() {
		return value;
	}

	public long timestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof CassandraColumn))
			return false;
		CassandraColumn other = (CassandraColumn) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

}
