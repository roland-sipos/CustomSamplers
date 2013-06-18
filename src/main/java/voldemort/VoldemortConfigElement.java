package voldemort;

import java.util.concurrent.TimeUnit;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.ClientConfig;

import utils.CustomSamplersException;

public class VoldemortConfigElement 
	extends AbstractTestElement
		implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 4973243869069060612L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String HOST = "VoldemortConfigElement.host";
	public final static String PORT = "VoldemortConfigElement.port";
	public final static String DATABASE = "VoldemortConfigElement.database";
	public final static String STORE = "VoldemortConfigElement.store";
	public final static String USERNAME = "VoldemortConfigElement.username";
	public final static String PASSWORD = "VoldemortConfigElement.password";
	
	public final static String MAX_THREADS = "VoldemortConfigElement.maxThreads";
	public final static String MAX_QUEUED_REQUESTS = "VoldemortConfigElement.maxQueueRequests";
	public final static String MAX_CONNECTIONS_PER_NODE = "VoldemortConfigElement.maxConnectionsPerNode";
	public final static String MAX_TOTAL_CONNECTIONS = "VoldemortConfigElement.maxTotalConnections";
	public final static String SOCKET_TIMEOUT = "VoldemortConfigElement.socketTimeout";
	public final static String CONNECTION_TIMEOUT = "VoldemortConfigElement.connectionTimeout";
	public final static String ROUTING_TIMEOUT = "VoldemortConfigElement.routingTimeout";
	public final static String SOCKET_BUFFER_SIZE = "VoldemortConfigElement.socketBufferSize";
	
	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}
		getThreadContext().getVariables().putObject(getDatabase(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	@SuppressWarnings("unchecked")
	public static StoreClient<String, byte[]> getVoldemortClient(String database, String store) 
			throws CustomSamplersException {
		Object voldemort = JMeterContextService.getContext().getVariables().getObject(database);
		if (voldemort == null) {
			throw new CustomSamplersException("Voldemort StoreClient object is null!");
		}
		else {
			if (voldemort instanceof StoreClient<?, ?>) {
				return (StoreClient<String, byte[]>)voldemort;
			}
			else {
				throw new CustomSamplersException("Casting the object to (StoreClient<String, String>) Voldemort client failed!");
			}
		}
	}
	
	@Override
	public void testStarted() {
		String fullHost = getHost() + ":" + getPort();
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}
		
		if (getThreadContext().getVariables().getObject(getDatabase()) != null) {
			log.warn(getDatabase() + " has already initialized!");
		} else {
			if (log.isDebugEnabled()) {
				log.debug(getDatabase() + " is being initialized ...");
			}
			
			ClientConfig cc = new ClientConfig();
			cc.setBootstrapUrls(fullHost);
			cc.setMaxThreads(Integer.parseInt(getMaxThreads()));
			cc.setMaxQueuedRequests(Integer.parseInt(getMaxQueuedRequests()));
			cc.setMaxConnectionsPerNode(Integer.parseInt(getMaxConnectionsPerNode()));
			cc.setMaxTotalConnections(Integer.parseInt(getMaxTotalConnections()));
			cc.setSocketTimeout(Integer.parseInt(getSocketTimeout()), TimeUnit.MILLISECONDS);
			cc.setConnectionTimeout(Integer.parseInt(getConnectionTimeout()), TimeUnit.MILLISECONDS);
			cc.setRoutingTimeout(Integer.parseInt(getRoutingTimeout()), TimeUnit.MILLISECONDS);
			cc.setSocketBufferSize(Integer.parseInt(getSocketBufferSize()));

			StoreClientFactory factory = new SocketStoreClientFactory(cc);
			StoreClient<String, String> store = factory.getStoreClient(getStore());
			getThreadContext().getVariables().putObject(getDatabase(), store);
		}
	}

	@Override
	public void testStarted(String arg0) {
		testStarted();	
	}

	@Override
	public void addConfigElement(ConfigElement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean expectsModification() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String getTitle() {
		return this.getName();
	}
	
	public String getHost() {
		return getPropertyAsString(HOST);
	}

	public void setHost(String host) {
		setProperty(HOST, host);
	}
	
	public String getPort() {
		return getPropertyAsString(PORT);
	}

	public void setPort(String port) {
		setProperty(PORT, port);
	}
	
	public String getDatabase() {
		return getPropertyAsString(DATABASE);
	}

	public void setDatabase(String database) {
		setProperty(DATABASE, database);
	}
	
	public String getStore() {
		return getPropertyAsString(STORE);
	}

	public void setStore(String store) {
		setProperty(STORE, store);
	}
	
	public String getUsername() {
		return getPropertyAsString(USERNAME);
	}

	public void setUsername(String username) {
		setProperty(USERNAME, username);
	}
	
	public String getPassword() {
		return getPropertyAsString(PASSWORD);
	}

	public void setPassword(String password) {
		setProperty(PASSWORD, password);
	}

	public String getMaxThreads() {
		return getPropertyAsString(MAX_THREADS);
	}

	public String getMaxQueuedRequests() {
		return getPropertyAsString(MAX_QUEUED_REQUESTS);
	}

	public String getMaxConnectionsPerNode() {
		return getPropertyAsString(MAX_CONNECTIONS_PER_NODE);
	}

	public String getMaxTotalConnections() {
		return getPropertyAsString(MAX_TOTAL_CONNECTIONS);
	}

	public String getSocketTimeout() {
		return getPropertyAsString(SOCKET_TIMEOUT);
	}

	public String getConnectionTimeout() {
		return getPropertyAsString(CONNECTION_TIMEOUT);
	}

	public String getRoutingTimeout() {
		return getPropertyAsString(ROUTING_TIMEOUT);
	}

	public String getSocketBufferSize() {
		return getPropertyAsString(SOCKET_BUFFER_SIZE);
	}

	public void setMaxThreads(String maxThreads) {
		setProperty(MAX_THREADS, maxThreads);
	}

	public void setMaxQueuedRequests(String maxQueuedRequests) {
		setProperty(MAX_QUEUED_REQUESTS, maxQueuedRequests);
	}

	public void setMaxConnectionsPerNode(String maxConnectionsPerNode) {
		setProperty(MAX_CONNECTIONS_PER_NODE, maxConnectionsPerNode);
	}

	public void setMaxTotalConnections(String maxTotalConnections) {
		setProperty(MAX_TOTAL_CONNECTIONS, maxTotalConnections);
	}

	public void setSocketTimeout(String socketTimeout) {
		setProperty(SOCKET_TIMEOUT, socketTimeout);
	}

	public void setConnectionTimeout(String connectionTimeout) {
		setProperty(CONNECTION_TIMEOUT, connectionTimeout);
	}

	public void setRoutingTimeout(String routingTimeout) {
		setProperty(ROUTING_TIMEOUT, routingTimeout);
	}

	public void setSocketBufferSize(String socketBufferSize) {
		setProperty(SOCKET_BUFFER_SIZE, socketBufferSize);
	}

}
