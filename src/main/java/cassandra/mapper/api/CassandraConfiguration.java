package cassandra.mapper.api;

public class CassandraConfiguration {

	private int connectionTimeout;
	private int maxActiveClients;
	private int maxIdleClients;
	private int maxWaitTimeWhenExhausted;
	private boolean useThriftFramedTransport;
	private boolean useLastInFirstOutPolicy;
	private boolean retryDownedNodes;
	private int timeSecondsBetweenRetryingDownedNodes;
	private boolean autoDiscoverNodes;
	private int timeSecondsBetweenNodeDiscovery;

	private CassandraConfiguration() {
		this.useThriftFramedTransport = false;
		this.useLastInFirstOutPolicy = true;
		this.retryDownedNodes = true;
		this.timeSecondsBetweenRetryingDownedNodes = 30;
		this.autoDiscoverNodes = false;
		this.timeSecondsBetweenNodeDiscovery = 30;
	}

	public CassandraConfiguration(int connectionTimeout, int maxActiveClients, int maxIdleClients,
			int maxWaitTimeWhenExhausted) {
		this();
		this.connectionTimeout = connectionTimeout;
		this.maxActiveClients = maxActiveClients;
		this.maxIdleClients = maxIdleClients;
		this.maxWaitTimeWhenExhausted = maxWaitTimeWhenExhausted;
	}

	public int connectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int maxActiveClients() {
		return maxActiveClients;
	}

	public void setMaxActiveClients(int maxActiveClients) {
		this.maxActiveClients = maxActiveClients;
	}

	public int maxIdleClients() {
		return maxIdleClients;
	}

	public void setMaxIdleClients(int maxIdleClients) {
		this.maxIdleClients = maxIdleClients;
	}

	public int maxWaitTimeWhenExhausted() {
		return maxWaitTimeWhenExhausted;
	}

	public void setMaxWaitTimeWhenExhausted(int maxWaitTimeWhenExhausted) {
		this.maxWaitTimeWhenExhausted = maxWaitTimeWhenExhausted;
	}

	public boolean useThriftFramedTransport() {
		return useThriftFramedTransport;
	}

	public void turnUseThriftFramedTransportOn() {
		this.useThriftFramedTransport = true;
	}

	public void turnUseThriftFramedTransportOff() {
		this.useThriftFramedTransport = false;
	}

	public boolean useLastInFirstOutPolicy() {
		return useLastInFirstOutPolicy;
	}

	public void turnUseLastInFirstOutPolicyOn() {
		this.useLastInFirstOutPolicy = true;
	}

	public void turnUseLastInFirstOutPolicyOff() {
		this.useLastInFirstOutPolicy = false;
	}

	public boolean retryDownedNodes() {
		return retryDownedNodes;
	}

	public void turnRetryDownedNodesOn(int timeSecondsBetweenRetryingDownedNodes) {
		this.retryDownedNodes = true;
		this.timeSecondsBetweenRetryingDownedNodes = timeSecondsBetweenRetryingDownedNodes;
	}

	public void turnRetryDownedNodesOff() {
		this.retryDownedNodes = false;
		this.timeSecondsBetweenRetryingDownedNodes = 30;
	}

	public int timeSecondsBetweenRetryingDownedNodes() {
		return timeSecondsBetweenRetryingDownedNodes;
	}

	public boolean autoDiscoverNodes() {
		return autoDiscoverNodes;
	}

	public void turnAutoDiscoverNodesOn(int timeSecondsBetweenNodeDiscovery) {
		this.autoDiscoverNodes = true;
		this.timeSecondsBetweenNodeDiscovery = timeSecondsBetweenNodeDiscovery;
	}

	public void turnAutoDiscoverNodesOff() {
		this.autoDiscoverNodes = false;
		this.timeSecondsBetweenNodeDiscovery = 30;
	}

	public int timeSecondsBetweenNodeDiscovery() {
		return timeSecondsBetweenNodeDiscovery;
	}

}
