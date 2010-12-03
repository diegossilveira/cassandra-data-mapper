package cassandra.mapper.api;

public class CassandraNode {

	private final String host;
	private final int port;

	public CassandraNode(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String host() {
		return host;
	}

	public int port() {
		return port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof CassandraNode))
			return false;
		CassandraNode other = (CassandraNode) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

}
