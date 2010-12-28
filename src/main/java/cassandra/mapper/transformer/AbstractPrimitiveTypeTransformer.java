package cassandra.mapper.transformer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import cassandra.mapper.api.Transformer;
import cassandra.mapper.api.exception.TransformerException;

public abstract class AbstractPrimitiveTypeTransformer implements Transformer {

	public byte[] toBytes(short value) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeShort(value);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public byte[] toBytes(int value) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeInt(value);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public byte[] toBytes(long value) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeLong(value);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public byte[] toBytes(float value) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeFloat(value);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public byte[] toBytes(double value) {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeDouble(value);
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public Short fromShortBytes(byte[] bytes) {

		try {
			return forBytes(bytes).readShort();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public Integer fromIntegertBytes(byte[] bytes) {

		try {
			return forBytes(bytes).readInt();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public Long fromLongBytes(byte[] bytes) {

		try {
			return forBytes(bytes).readLong();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public Float fromFloatBytes(byte[] bytes) {

		try {
			return forBytes(bytes).readFloat();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	public Double fromDoubleBytes(byte[] bytes) throws TransformerException {

		try {
			return forBytes(bytes).readDouble();
		} catch (Exception ex) {
			throw new TransformerException(ex);
		}
	}

	private DataInputStream forBytes(byte[] bytes) {

		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		return new DataInputStream(bais);
	}

}
