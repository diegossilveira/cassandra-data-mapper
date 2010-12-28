package cassandra.mapper.transformer;

import cassandra.mapper.api.exception.TransformerException;

public class DoubleTransformer extends AbstractPrimitiveTypeTransformer {

	@Override
	public byte[] toBytes(Object object) {

		try {
			
			double value = ((Double) object).doubleValue();
			return toBytes(value);

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Double fromBytes(byte[] bytes) {

		try {

			return fromDoubleBytes(bytes);

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Long", ex);
		}
	}
	
}
