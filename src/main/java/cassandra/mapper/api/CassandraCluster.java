package cassandra.mapper.api;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CassandraCluster {

	private final String name;
	private final Set<CassandraNode> nodes;
	private final CassandraConfiguration config;

	public CassandraCluster(String name, CassandraConfiguration config) {
		this.name = name;
		this.nodes = new HashSet<CassandraNode>();
		this.config = config;
	}

	public CassandraCluster(String name, CassandraConfiguration config, Collection<CassandraNode> nodes) {
		this(name, config);
		nodes.addAll(nodes);
	}

	public String name() {
		return name;
	}
	
	public int size() {
		return nodes.size();
	}

	public Collection<CassandraNode> nodes() {
		return nodes;
	}
	
	public CassandraCluster addNode(CassandraNode node) {
		nodes.add(node);
		return this;
	}
	
	public CassandraCluster removeNode(CassandraNode node) {
		nodes.remove(node);
		return this;
	}

	public CassandraConfiguration config() {
		return config;
	}
	
}
