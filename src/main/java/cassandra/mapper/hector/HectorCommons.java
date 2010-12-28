package cassandra.mapper.hector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.engine.EntityProcessor;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;

public final class HectorCommons {
	
	private static final Logger logger = Logger.getLogger(HectorCommons.class);
	
	private HectorCommons() {
	}
	
	static Row<String, byte[]> getLastRow(OrderedRows<String, byte[]> rows) {

		List<Row<String, byte[]>> list = rows.getList();
		return list.get(list.size() - 1);
	}

	private static CassandraColumn convert(HColumn<String, byte[]> column) {

		CassandraColumn cassandraColumn = new CassandraColumn(column.getName(), column.getValue());
		cassandraColumn.setTimestamp(column.getTimestamp());
		return cassandraColumn;
	}

	private static CassandraIndexColumn convertIndex(HColumn<UUID, UUID> column) {

		CassandraIndexColumn cassandraIndexColumn = new CassandraIndexColumn(column.getName(), column.getValue());
		cassandraIndexColumn.setTimestamp(column.getTimestamp());
		return cassandraIndexColumn;
	}
	
	private static boolean mustIgnoreKey(UUID ignoredKey, String rowKey) {
		
		return ignoredKey != null && ignoredKey.toString().equals(rowKey);
	}

	static <E> E buildEntity(ColumnSlice<String, byte[]> slice, UUID key, EntityProcessor<E> processor, boolean lazy) {

		logger.debug("Deserializing entity with key " + key);
		
		List<CassandraColumn> columns = new ArrayList<CassandraColumn>();

		for (HColumn<String, byte[]> column : slice.getColumns()) {
			columns.add(convert(column));
		}

		return processor.getCassandraEntity(key, columns, lazy);
	}

	static <E> List<E> buildEntities(Rows<String, byte[]> rows, UUID ignoredKey, EntityProcessor<E> processor, boolean lazy) {

		List<E> entities = new ArrayList<E>();
		Iterator<Row<String, byte[]>> iterator = rows.iterator();

		while (iterator.hasNext()) {
			Row<String, byte[]> row = iterator.next();
			if (!mustIgnoreKey(ignoredKey, row.getKey())) {
				E entity = buildEntity(row.getColumnSlice(), UUID.fromString(row.getKey()), processor, lazy);
				entities.add(entity);
			}
		}

		return entities;
	}

	static List<CassandraIndexColumn> buildIndexes(ColumnSlice<UUID, UUID> slice) {

		logger.debug("Building index columns");

		List<CassandraIndexColumn> columns = new ArrayList<CassandraIndexColumn>();

		for (HColumn<UUID, UUID> column : slice.getColumns()) {
			columns.add(convertIndex(column));
		}

		return columns;
	}
	
	static String[] toStringKeys(Collection<UUID> keys) {
		
		String[] array = new String[keys.size()];
		int i = 0;
		for(UUID uuid : keys) {
			array[i++] = uuid.toString();
		}
		
		return array;
	}
	
}
