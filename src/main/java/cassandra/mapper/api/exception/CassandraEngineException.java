package cassandra.mapper.api.exception;

public class CassandraEngineException extends RuntimeException {

	private static final long serialVersionUID = -2905272134669171711L;

	public CassandraEngineException() {
	}

	public CassandraEngineException(String message, Throwable cause) {
		super(message, cause);
	}

	public CassandraEngineException(String message) {
		super(message);
	}

	public CassandraEngineException(Throwable cause) {
		super(cause);
	}

}
