package cassandra;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;

public class CassandraConfigElement
	extends AbstractTestElement
		implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 100179183283120561L;

	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String HOST = "CassandraConfigElement.host"; //DEF: 188.184.23.11
	public final static String PORT = "CassandraConfigElement.port"; // 9160
	public final static String CLUSTER = "CassandraConfigElement.cluster";
	public final static String THRIFT_SOCKET_TIMEOUT = "CassandraConfigElement.thriftSocketTimeout"; //def: 10000
	public final static String RETRY_DOWNED_HOST_DELAY_SEC = "CassandraConfigElement.retryDownedHostDelaySec"; // def: 30
	public final static String RETRY_DOWNED_HOST_QUEUE_SIZE = "CassandraConfigElement.retryDownedHostQueueSize"; // def: 128
	public final static String MAX_WAIT_TIME_IF_EXHAUSTED = "CassandraConfigElement.maxWaitTimeIfExhausted"; //def:60000
	public final static String RETRY_DOWNED_HOSTS = "CassandraConfigElement.retryDownedHosts"; //def: true
	public final static String LOAD_BALANCING_POLICY = "CassandraConfigElement.loadBalancingPolicy"; //def: LeastActiveBalancing
	public final static String SET_AUTO_DISCOVER_HOSTS = "CassandraConfigElement.setAutoDiscoverHosts"; //def: true
	public final static String SET_HOST_TIMEOUT_COUNTER = "CassandraConfigElement.setHostTimeoutCounter"; //def: 20
	
	
	public static Cluster getCassandraCluster(String cluster) throws CustomSamplersException {
		Object cassandra = JMeterContextService.getContext().getVariables().getObject(cluster);
		if (cassandra == null) {
			throw new CustomSamplersException("Cassandra cluster object is null!");
		}
		else {
			if (cassandra instanceof Cluster) {
				return (Cluster)cassandra;
			}
			else {
				throw new CustomSamplersException("Casting the object to Cluster failed!");
			}
		}
	}
	
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
		}
		String fullHost = new String(getHost() + ":" + getPort());
		
		CassandraHostConfigurator conf = new CassandraHostConfigurator(fullHost);
		conf.setCassandraThriftSocketTimeout(Integer.parseInt(getThriftSocketTimeout()));
		conf.setRetryDownedHostsDelayInSeconds(Integer.parseInt(getRetryDownedHostDelaySec()));
		conf.setRetryDownedHostsQueueSize(Integer.parseInt(getRetryDownedHostQueueSize()));
		conf.setMaxWaitTimeWhenExhausted(Integer.parseInt(getMaxWaitTimeIfExhausted()));
		conf.setRetryDownedHosts(Boolean.parseBoolean(getRetryDownedHosts()));
		conf.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		conf.setAutoDiscoverHosts(Boolean.parseBoolean(getSetAutoDiscoverHosts()));
		conf.setHostTimeoutCounter(Integer.parseInt(getSetHostTimeoutCounter()));
		
		if (log.isDebugEnabled()) {
			log.debug("Cassandra host config: " + conf.toString());
		}
		
		if (getThreadContext().getVariables().getObject(getCluster()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getCluster() + " has already been defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getCluster() + " is being defined...");
			}
			Cluster cluster = HFactory.getOrCreateCluster(getCluster(), fullHost);
			// This will be the real use-case, when the host itself is configured:
			// Cluster confedCluster = HFactory.getOrCreateCluster(getCluster(), conf);
			getThreadContext().getVariables().putObject(getCluster(), cluster);
		}
		
	}

	@Override
	public void testStarted(String arg0) {
		testStarted();
	}
	
	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}
		getThreadContext().getVariables().putObject(getCluster(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	@Override
	public void addConfigElement(ConfigElement arg0) {
		
	}

	@Override
	public boolean expectsModification() {
		return false;
	}
	
	public String getTitle() {
		return this.getName();
	}
	
	public String getHost() {
		return getPropertyAsString(HOST);
	}

	public String getPort() {
		return getPropertyAsString(PORT);
	}
	
	public String getCluster() {
		return getPropertyAsString(CLUSTER);
	}

	public String getThriftSocketTimeout() {
		return getPropertyAsString(THRIFT_SOCKET_TIMEOUT);
	}

	public String getRetryDownedHostDelaySec() {
		return getPropertyAsString(RETRY_DOWNED_HOST_DELAY_SEC);
	}

	public String getRetryDownedHostQueueSize() {
		return getPropertyAsString(RETRY_DOWNED_HOST_QUEUE_SIZE);
	}

	public String getMaxWaitTimeIfExhausted() {
		return getPropertyAsString(MAX_WAIT_TIME_IF_EXHAUSTED);
	}

	public String getRetryDownedHosts() {
		return getPropertyAsString(RETRY_DOWNED_HOSTS);
	}

	public String getLoadBalancingPolicy() {
		return getPropertyAsString(LOAD_BALANCING_POLICY);
	}

	public String getSetAutoDiscoverHosts() {
		return getPropertyAsString(SET_AUTO_DISCOVER_HOSTS);
	}

	public String getSetHostTimeoutCounter() {
		return getPropertyAsString(SET_HOST_TIMEOUT_COUNTER);
	}

	public void setHost(String host) {
		setProperty(HOST, host);
	}

	public void setPort(String port) {
		setProperty(PORT, port);
	}

	public void setCluster(String cluster) {
		setProperty(CLUSTER, cluster);
	}

	public void setThriftSocketTimeout(String thriftSocketTimeout) {
		setProperty(THRIFT_SOCKET_TIMEOUT, thriftSocketTimeout);
	}

	public void setRetryDownedHostDelaySec(String retryDownedHostDelaySec) {
		setProperty(RETRY_DOWNED_HOST_DELAY_SEC, retryDownedHostDelaySec);
	}

	public void setRetryDownedHostQueueSize(String retryDownedHostQueueSize) {
		setProperty(RETRY_DOWNED_HOST_QUEUE_SIZE, retryDownedHostQueueSize);
	}

	public void setMaxWaitTimeIfExhausted(String maxWaitTimeIfExhausted) {
		setProperty(MAX_WAIT_TIME_IF_EXHAUSTED, maxWaitTimeIfExhausted);
	}

	public void setRetryDownedHosts(String retryDownedHosts) {
		setProperty(RETRY_DOWNED_HOSTS, retryDownedHosts);
	}

	public void setLoadBalancingPolicy(String loadBalancingPolicy) {
		setProperty(LOAD_BALANCING_POLICY, loadBalancingPolicy);
	}

	public void setSetAutoDiscoverHosts(String setAutoDiscoverHosts) {
		setProperty(SET_AUTO_DISCOVER_HOSTS, setAutoDiscoverHosts);
	}

	public void setSetHostTimeoutCounter(String setHostTimeoutCounter) {
		setProperty(SET_HOST_TIMEOUT_COUNTER, setHostTimeoutCounter);
	}
	
}
