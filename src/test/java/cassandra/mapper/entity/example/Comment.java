package cassandra.mapper.entity.example;

import java.util.Date;
import java.util.UUID;

import cassandra.mapper.api.annotation.Column;
import cassandra.mapper.api.annotation.Entity;
import cassandra.mapper.api.annotation.Index;
import cassandra.mapper.api.annotation.Key;

@Entity(keyspace = "CommentKeyspace", columnFamily = "Comment")
public class Comment {

	@Key
	private final UUID id;
	@Column
	@Index(name = "commentedBy", columnFamily = "CommentByAuthor")
	private final String author;
	@Column
	private String text;
	@Column
	private final Date date = new Date();
	@Column
	private int count = 10;

	public Comment() {
		this(null, "");
	}

	public Comment(UUID id, String author) {
		this(id, author, "");
	}

	public Comment(UUID id, String author, String text) {
		this.id = id;
		this.author = author;
		this.text = text;
	}

	public void changeText(String text) {
		this.text = text;
	}

	public UUID id() {
		return this.id;
	}

	public String author() {
		return this.author;
	}

	public String text() {
		return this.text;
	}

	public Date date() {
		return date;
	}

	public int count() {
		return count;
	}

	public void incrementCount() {
		this.count++;
	}

	public void decrementCount() {
		this.count--;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((author == null) ? 0 : author.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((text == null) ? 0 : text.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof Comment))
			return false;
		Comment other = (Comment) obj;
		if (author == null) {
			if (other.author != null)
				return false;
		} else if (!author.equals(other.author))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Comment [id=" + id + ", author=" + author + ", text=" + text + "]";
	}

}
