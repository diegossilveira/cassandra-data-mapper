package cassandra.mapper.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.TransformerException;

public class ObjectTransformer implements Transformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);

			return baos.toByteArray();

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Object fromBytes(byte[] bytes) {

		try {

			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Object", ex);
		}
	}
	
}
