package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class FloatTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			float value = ((Float) object).floatValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Float fromBytes(byte[] bytes) {

		try {

			return fromFloatBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Float", ex);
		}
	}

}
