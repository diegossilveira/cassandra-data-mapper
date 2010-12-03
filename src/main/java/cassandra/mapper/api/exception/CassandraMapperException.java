package cassandra.mapper.api.exception;

public class CassandraMapperException extends RuntimeException {

	private static final long serialVersionUID = 4836839102015254375L;

	public CassandraMapperException() {
	}

	public CassandraMapperException(String message, Throwable cause) {
		super(message, cause);
	}

	public CassandraMapperException(String message) {
		super(message);
	}

	public CassandraMapperException(Throwable cause) {
		super(cause);
	}

}
