package cassandra.mapper.engine;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import cassandra.mapper.api.CassandraColumn;
import cassandra.mapper.api.CassandraIndexColumn;
import cassandra.mapper.engine.EntityProcessor;
import cassandra.mapper.engine.EntityProcessorFactory;
import cassandra.mapper.entity.example.Comment;
import cassandra.mapper.transformer.StringTransformer;


public class EntityProcessorTest {

	private Comment comment;
	private StringTransformer transformer;
	private EntityProcessor<Comment> processor;

	@Before
	public void init() {
		comment = new Comment(toUUID(1), "diegossilveira", "Comentario de Teste");
		transformer = new StringTransformer();
		processor = EntityProcessorFactory.getEntityProcessor(Comment.class);
	}
	
	private UUID toUUID(int num) {
		return UUID.nameUUIDFromBytes(String.valueOf(num).getBytes());
	}

	@Test
	public void testGetCassandraColumns() {

		Collection<CassandraColumn> columns = processor.getCassandraColumns(comment);
		assertArrayEquals(new CassandraColumn[] { new CassandraColumn("author", comment.author(), transformer),
				new CassandraColumn("text", comment.text(), transformer) }, columns.toArray(new CassandraColumn[0]));
	}

	@Test
	public void testGetKeyspace() {

		assertEquals("KeyspaceTest", processor.getKeyspace());
	}

	@Test
	public void testGetColumnFamily() {

		assertEquals("Comment", processor.getColumnFamily());
	}

	@Test
	public void testGetCassandraEntity() {
		
		CassandraColumn[] columns = new CassandraColumn[] { new CassandraColumn("author", comment.author(), transformer),
				new CassandraColumn("text", comment.text(), transformer) };
		Comment commentReturned = processor.getCassandraEntity(toUUID(1), Arrays.asList(columns));
		assertEquals(comment, commentReturned);
	}
	
	@Test
	public void testGetColumnCount() {
		
		assertEquals(2, processor.getColumnCount());
	}
	
	@Test
	public void testGetCassandraIndexColumn() {

		CassandraIndexColumn column = processor.getCassandraIndexColumn(comment);
		assertEquals(toUUID(1), column.indexedKey());
	}
	
	@Test
	public void testGetIndexColumnFamily() {

		assertEquals("CommentByAuthor", processor.getIndexColumnFamily("commentedBy"));
	}
	
	@Test
	public void testGetIndexCount() {
		
		assertEquals(1, processor.getIndexCount());
	}
	
}
