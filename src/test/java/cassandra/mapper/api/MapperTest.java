package cassandra.mapper.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import cassandra.mapper.entity.example.Comment;
import cassandra.mapper.hector.HectorBasedMapper;


public class MapperTest {

	private Mapper mapper;
	private Logger log = Logger.getLogger(MapperTest.class);

	@Before
	public void init() {

		CassandraConfiguration configuration = new CassandraConfiguration(5000, 10, -1, -1);
		CassandraCluster cluster = new CassandraCluster("TQI-cluster", configuration);
		cluster.addNode(new CassandraNode("10.10.0.69", 9160));
		cluster.addNode(new CassandraNode("10.10.0.70", 9160));

		mapper = new HectorBasedMapper(cluster);
	}

	private UUID toUUID(int num) {
		return UUID.nameUUIDFromBytes(String.valueOf(num).getBytes());
	}

	// @Test
	public void testStore() {

		Comment comment = new Comment(toUUID(1), "diegossilveira", "Comentario de Teste");
		mapper.store(comment);
		assertEquals(comment, mapper.findByKey(toUUID(1), Comment.class));
		mapper.remove(comment);
	}

	//@Test
	public void testSerialMassiveStore() {

		for (int i = 1; i <= 100; i++) {
			Comment comment = new Comment(toUUID(i), "diegossilveira", "Comentario de Teste " + i);
			mapper.store(comment);
		}

		CassandraRange<Comment> range = mapper.findByRange(null, Comment.class, 100);
		assertEquals(100, range.size());

		for (int i = 1; i <= 100; i++) {
			mapper.remove(toUUID(i), Comment.class);
		}
	}

	// @Test
	public void testParallelMassiveStore() {

		Executor executor = Executors.newFixedThreadPool(10);

		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				int id = (int) Thread.currentThread().getId();

				for (int i = 0; i < 1000; i++) {
					Comment comment = new Comment(toUUID(i), "diegossilveira", "Comentario de Teste " + id + "." + i);
					log.info("Saving: " + comment.toString());
					mapper.store(comment);
				}
			}
		};

		for (int i = 0; i < 10; i++) {
			executor.execute(runnable);
		}

		// System.out.println(mapper.findByKey(250, Comment.class));
	}

	// @Test
	public void testStoringFindingAndRemoving() {

		Comment comment = new Comment(toUUID(1), "diegossilveira", "Removing Test");
		mapper.store(comment);

		assertTrue(mapper.findByKey(toUUID(1), Comment.class).text().equals("Removing Test"));

		mapper.remove(toUUID(1), Comment.class);

		assertTrue(mapper.findByKey(toUUID(1), Comment.class).text().equals(""));
	}

	// @Test
	public void testStoringAndFindingByKeysInBatch() {

		List<UUID> keys = new ArrayList<UUID>();
		for (int i = 0; i < 5; i++) {
			keys.add(toUUID(i));
		}
		List<Comment> list = mapper.findByKeys(keys, Comment.class);
		assertEquals(5, list.size());
	}

	//@Test
	public void testFindingByRange() {

		UUID nextKey = null;
		CassandraRange<Comment> range = mapper.findByRange(nextKey, Comment.class, 1);
		long count = 0;
		while (range.hasNextKey()) {
			range = mapper.findByRange(nextKey, Comment.class, 3000);
			nextKey = range.nextKey();
			count += range.size();
		}
		System.out.println(count);
		assertTrue(count > 0);
	}
	
	@Test
	public void testProxy() {
		
//		for (int i = 1; i <= 100; i++) {
//			Comment comment = new Comment(toUUID(i), "diegossilveira", "Comentario de Teste " + i);
//			mapper.store(comment);
//		}
		
		Comment comment1 = new Comment(toUUID(1), "diegossilveira", null);
		mapper.store(comment1);
		
		CassandraRange<Comment> range = mapper.findByRange(null, Comment.class, 100, FetchMode.LAZY);
		for(Comment comment : range.elements()) {
			comment.id();
			comment.author();
			comment.text();
			comment.date();
		}
		
		assertEquals(100, range.size());
	}

}
