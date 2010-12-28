package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class ShortTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			short value = ((Short) object).shortValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Short fromBytes(byte[] bytes) {

		try {

			return fromShortBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Short", ex);
		}
	}

}
