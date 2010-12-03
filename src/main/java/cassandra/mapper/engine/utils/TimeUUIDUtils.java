package cassandra.mapper.engine.utils;

import java.util.Date;
import java.util.UUID;

public abstract class TimeUUIDUtils {

	/**
	 * TimeUUID (UUID version 1) considers a timestamp given in unit of 100-nanosecond since the institution of the
	 * Gregorian Calendar (October 15, 1582). The java.util.Date object considers January 1st, 1970 as base data. This
	 * field is the difference between those dates in miliseconds.
	 */
	private static final long TIME_UUID_MILISECONDS_OFFSET = 12219292800000L;

	public static UUID getTimeUUID() {

		com.eaio.uuid.UUID uuid = new com.eaio.uuid.UUID();
		return UUID.fromString(uuid.toString());
	}

	public static UUID toUUID(byte[] uuid) {

		long msb = 0;
		long lsb = 0;

		for (int i = 0; i < 8; i++) {
			msb = (msb << 8) | (uuid[i] & 0xff);
		}
		for (int i = 8; i < 16; i++) {
			lsb = (lsb << 8) | (uuid[i] & 0xff);
		}

		com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb, lsb);
		return UUID.fromString(u.toString());
	}

	public static byte[] toByteArray(UUID uuid) {

		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}

	public static Date toDate(UUID timeUUID) {

		long timestamp = timeUUID.timestamp() / 10000;
		return new Date(timestamp - TIME_UUID_MILISECONDS_OFFSET);
	}

}
