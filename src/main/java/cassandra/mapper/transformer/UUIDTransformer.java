package cassandra.mapper.transformer;

import java.util.UUID;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.TransformerException;
import cassandra.mapper.engine.utils.TimeUUIDUtils;

public class UUIDTransformer implements Transformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			return TimeUUIDUtils.toByteArray((UUID) object);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public UUID fromBytes(byte[] bytes) {

		try {

			return TimeUUIDUtils.toUUID(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into UUID", ex);
		}
	}

}
