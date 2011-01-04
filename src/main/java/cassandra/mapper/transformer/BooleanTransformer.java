package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class BooleanTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {
			
			boolean value = ((Boolean) object).booleanValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Boolean fromBytes(byte[] bytes) {

		try {

			return fromBooleanBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Boolean", ex);
		}
	}

}
