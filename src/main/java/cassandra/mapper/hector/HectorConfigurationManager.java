package cassandra.mapper.hector;

import cassandra.mapper.api.CassandraCluster;
import cassandra.mapper.api.CassandraConfiguration;
import cassandra.mapper.api.CassandraNode;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ExhaustedPolicy;

class HectorConfigurationManager {

	private final CassandraHostConfigurator configurator;

	HectorConfigurationManager(CassandraCluster cluster) {
		configurator = new CassandraHostConfigurator();
		configurator.setHosts(getHostsString(cluster));
		configure(cluster.config());
	}

	void configure(CassandraConfiguration config) {

		configurator.setCassandraThriftSocketTimeout(config.connectionTimeout());
		configurator.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
		configurator.setLifo(config.useLastInFirstOutPolicy());
		configurator.setMaxActive(config.maxActiveClients());
		configurator.setMaxIdle(config.maxIdleClients());
		configurator.setUseThriftFramedTransport(config.useThriftFramedTransport());
		configurator.setMaxWaitTimeWhenExhausted(config.maxWaitTimeWhenExhausted());
	}
	
	private String getHostsString(CassandraCluster cassandraCluster) {
		StringBuilder hosts = new StringBuilder();
		for (CassandraNode node : cassandraCluster.nodes()) {
			hosts.append(node.host()).append(":").append(node.port()).append(",");
		}
		if (hosts.length() > 0) {
			hosts.delete(hosts.length() - 1, hosts.length());
		}

		return hosts.toString();
	} 

	CassandraHostConfigurator configurator() {
		return configurator;
	}

}
