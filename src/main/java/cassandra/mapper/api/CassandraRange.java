package cassandra.mapper.api;

import java.util.List;
import java.util.UUID;

public class CassandraRange<E> {

	private final List<E> elements;
	private final UUID nextKey;

	public CassandraRange(List<E> elements, UUID nextKey) {
		this.elements = elements;
		this.nextKey = nextKey;
	}

	public List<E> elements() {
		return elements;
	}

	public UUID nextKey() {
		return nextKey;
	}

	public boolean hasNextKey() {
		return nextKey != null;
	}

	public int size() {
		return elements.size();
	}

	public boolean isEmpty() {
		return elements.isEmpty();
	}

	public boolean gotLastKey() {
		return !isEmpty() && !hasNextKey();
	}

}
