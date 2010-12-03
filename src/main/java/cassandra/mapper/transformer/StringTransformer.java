package cassandra.mapper.transformer;

import java.io.UnsupportedEncodingException;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.TransformerException;


public class StringTransformer implements Transformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			return ((String) object).getBytes("UTF-8");

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object
					.getClass().getCanonicalName()), ex);
		}
	}

	@Override
	public String fromBytes(byte[] bytes) {

		try {

			return new String(bytes, "UTF-8");

		} catch (UnsupportedEncodingException ex) {
			throw new TransformerException("Unable to transform byte[] into String", ex);
		}
	}

}
