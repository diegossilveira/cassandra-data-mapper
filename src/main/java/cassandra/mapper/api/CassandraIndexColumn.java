package cassandra.mapper.api;

import java.util.UUID;

public class CassandraIndexColumn {

	private final UUID indexer;

	private final UUID indexedKey;
	
	private long timestamp;

	public CassandraIndexColumn(UUID indexer, UUID indexedKey) {
		this.indexer = indexer;
		this.indexedKey = indexedKey;
	}

	public UUID indexer() {
		return indexer;
	}

	public UUID indexedKey() {
		return indexedKey;
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
		result = prime * result + ((indexedKey == null) ? 0 : indexedKey.hashCode());
		result = prime * result + ((indexer == null) ? 0 : indexer.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof CassandraIndexColumn))
			return false;
		CassandraIndexColumn other = (CassandraIndexColumn) obj;
		if (indexedKey == null) {
			if (other.indexedKey != null)
				return false;
		} else if (!indexedKey.equals(other.indexedKey))
			return false;
		if (indexer == null) {
			if (other.indexer != null)
				return false;
		} else if (!indexer.equals(other.indexer))
			return false;
		return true;
	}

}
