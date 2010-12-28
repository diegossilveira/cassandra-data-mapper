package cassandra.mapper.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.TransformerException;

public class DateTransformer implements Transformer {

	@Override
	public byte[] toBytes(Object object) {

		try {

			Date date = ((Date) object);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(date);

			return baos.toByteArray();

		} catch (Exception ex) {
			throw new TransformerException(String.format("Unable to transform object of type '%s' into byte[]", object.getClass()
					.getCanonicalName()), ex);
		}
	}

	@Override
	public Date fromBytes(byte[] bytes) {

		try {

			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return (Date) ois.readObject();

		} catch (Exception ex) {
			throw new TransformerException("Unable to transform byte[] into Date", ex);
		}
	}

}
