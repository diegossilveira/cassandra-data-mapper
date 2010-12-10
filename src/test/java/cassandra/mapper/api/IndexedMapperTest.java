package cassandra.mapper.api;

import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import cassandra.mapper.api.CassandraCluster;
import cassandra.mapper.api.CassandraConfiguration;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.api.CassandraNode;
import cassandra.mapper.api.IndexedMapper;
import cassandra.mapper.engine.utils.TimeUUIDUtils;
import cassandra.mapper.entity.example.Comment;
import cassandra.mapper.hector.HectorBasedMapper;


import static org.junit.Assert.*;

public class IndexedMapperTest {

	private IndexedMapper mapper;

	@Before
	public void init() {

		CassandraConfiguration configuration = new CassandraConfiguration(5000, 10, -1, -1);
		CassandraCluster cluster = new CassandraCluster("TQI-cluster", configuration);
		cluster.addNode(new CassandraNode("10.10.0.69", 9160));
		cluster.addNode(new CassandraNode("10.10.0.70", 9160));
		cluster.addNode(new CassandraNode("10.10.0.71", 9160));

		mapper = new HectorBasedMapper(cluster);
	}
	
	private UUID toUUID(int num) {
		return UUID.nameUUIDFromBytes(String.valueOf(num).getBytes());
	}

	//@Test
	public void testUpdateIndex() {

		String[] authors = { "diegossilveira", "nayarasilveira", "lorenabittencourt" };

		for (int i = 200; i <= 500; i++) {
			Comment comment = new Comment(TimeUUIDUtils.getTimeUUID(), authors[i % 3], "My comment " + i / 3);
			mapper.store(comment);
			mapper.updateIndex(comment, "commentedBy");
		}
	}

	//@Test
	public void testRemoveIdexedColumn() {
		
		CassandraIndexColumn indexColumn = new CassandraIndexColumn(UUID.fromString("26d41600-fe4b-11df-b39b-001cc0c3e436"), toUUID(96));
		mapper.removeIndexedColumn(Comment.class, "commentedBy", "lorenabittencourt", indexColumn);
		
		List<CassandraIndexColumn> indexedColumns = mapper.findIndexedColumns(Comment.class, "commentedBy", "lorenabittencourt");
		assertFalse(indexedColumns.contains(indexColumn));
	}

	//@Test
	public void testFindIndexedColumns() {

		List<CassandraIndexColumn> indexedColumns = mapper.findIndexedColumns(Comment.class, "commentedBy",
				"lorenabittencourt");
		for (CassandraIndexColumn indexColumn : indexedColumns) {
			System.out.println(TimeUUIDUtils.toDate(indexColumn.indexer()) + ": " + indexColumn.indexedKey());
		}
	}

}
