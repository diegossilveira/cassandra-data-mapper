package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class IntegerTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {
			
			int value = ((Integer) object).intValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Integer fromBytes(byte[] bytes) {

		try {

			return fromIntegertBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Integer", ex);
		}
	}

}
