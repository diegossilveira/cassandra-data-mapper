package cassandra.mapper.api.exception;

public class TransformerException extends RuntimeException {

	private static final long serialVersionUID = 7087841158135819054L;

	public TransformerException() {
	}

	public TransformerException(String message, Throwable cause) {
		super(message, cause);
	}

	public TransformerException(String message) {
		super(message);
	}

	public TransformerException(Throwable cause) {
		super(cause);
	}

}
