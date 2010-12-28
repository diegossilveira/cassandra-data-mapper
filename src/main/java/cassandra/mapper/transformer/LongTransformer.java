package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class LongTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			long value = ((Long) object).longValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Long fromBytes(byte[] bytes) {

		try {

			return fromLongBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Long", ex);
		}
	}

}
