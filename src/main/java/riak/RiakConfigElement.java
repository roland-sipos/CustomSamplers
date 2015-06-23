package riak;

import java.net.UnknownHostException;
import java.util.List;
import java.util.LinkedList;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

import utils.CustomSamplersException;

/*import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.NodeStats;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;*/


public class RiakConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -4956133291185363007L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CONNECTION_ID = "RiakConfigElement.connectionId";
	public final static String HOST = "RiakConfigElement.host";
	public final static String PORT = "RiakConfigElement.port";
	public final static String CLUSTER = "RiakConfigElement.cluster";
	public final static String USERNAME = "RiakConfigElement.username";
	public final static String PASSWORD = "RiakConfigElement.password";

	public final static String PROTOCOL = "RiakConfigElement.protocol";
	public final static String MAX_CONNECTION = "RiakConfigElement.maxConnection";
	public final static String TIMEOUT = "RiakConfigElement.timeout";

	private static String protocol;

	
	@Override
	public void testEnded() {
		RiakClient riak = null;
		try {
			riak = getRiakClient(getConnectionId());
		} catch (CustomSamplersException e) {
			log.error("Unable to get the RiakClient for Shutdown in testEnd()!");
		} finally {
			System.out.println(" Attempt on shutting down RiakClient ... ");
			riak.shutdown();
			System.out.println(" ... Success!");
		}
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static RiakClient getRiakClient(String connectionId) throws CustomSamplersException {
		Object riak = JMeterContextService.getContext().getVariables().getObject(connectionId);
		if (riak == null) {
			throw new CustomSamplersException("RiakClient object is null!");
		}
		else {
			if (riak instanceof RiakClient) {
				return (RiakClient)riak;
			}
			else {
				throw new CustomSamplersException("Casting the object to RiakClient failed!");
			}
		}
	}

	/*public static Bucket getBucket(String connectionId) throws CustomSamplersException {
		Object bucket = JMeterContextService.getContext().getVariables().getObject(connectionId);
		if (bucket == null) {
			throw new CustomSamplersException("Bucket object is null!");
		}
		else {
			if (bucket instanceof Bucket) {
				return (Bucket)bucket;
			}
			else {
				throw new CustomSamplersException("Casting the object to Bucket failed!");
			}
		}
	}*/

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}

		//IRiakClient riakClient = null;
		/*if (getProtocol().equals("HTTP")) {
			protocol = "HTTP";
			HTTPClientConfig.Builder configBuilder = new HTTPClientConfig.Builder();
			//HTTPClientConfig.Builder.from(HTTPClientConfig.defaults());
			configBuilder.withHost(hosts[0]);
			configBuilder.withPort(Integer.parseInt(getPort()));
			//configBuilder.withTimeout(Integer.parseInt(getTimeout()));
			//configBuilder.withHttpClient(client)

			HTTPClientConfig clientConfig = configBuilder.build();			
			HTTPClusterConfig clusterConf = new HTTPClusterConfig(Integer.parseInt(getMaxConnection()));
			clusterConf.addClient(clientConfig);
			for (int i = 0; i < hosts.length; ++i) {
				System.out.println("Adding host to HTTP cluster config: " + hosts[i]);
				clusterConf.addHosts(hosts[i]);
			}

			try {
				riakClient = RiakFactory.newClient(clusterConf);
			} catch (RiakException e) {
				log.error("RiakFactory was unabled to create a new client " 
						+ "based on the HTTPClusterConfig: " + e.toString());
			}


		} else if (getProtocol().equals("RAW-PB")) {
			protocol = "RAW-PB";
			PBClientConfig.Builder configBuilder = 
					PBClientConfig.Builder.from(PBClientConfig.defaults());
			configBuilder.withHost(hosts[0]);
			configBuilder.withPort(Integer.parseInt(getPort()));
			configBuilder.withRequestTimeoutMillis(Integer.parseInt(getTimeout()));

			PBClientConfig clientConfig = configBuilder.build();
			PBClusterConfig clusterConf = new PBClusterConfig(Integer.parseInt(getMaxConnection()));
			clusterConf.addClient(clientConfig);
			for (int i = 0; i < hosts.length; ++i) {
				System.out.println("Adding host to RAW-PB cluster config: " + hosts[i]);
				clusterConf.addHosts(hosts[i]);
			}
			System.out.println("On this IRiakClient max connection number is: "
					+ clusterConf.getTotalMaximumConnections());

			try {
				riakClient = RiakFactory.newClient(clusterConf);
			} catch (RiakException e) {
				log.error("RiakFactory was unabled to create a new client " 
						+ "based on the PBClusterConfig: " + e.toString());
			}

		} else {
			log.error("Unknown protocol requested for the connection builder!");
		}*/

		RiakNode.Builder builder = new RiakNode.Builder();
		builder.withMinConnections(10);
		builder.withMaxConnections(50);

		String[] hosts = getHost().split(",");
		LinkedList<String> addresses = new LinkedList<String>();
		for (int i = 0; i < hosts.length; ++i) {
			System.out.println("Adding host to cluster config: " + hosts[i]);
			addresses.add(hosts[i]);
		}

		RiakClient riakClient = null;
		try {
			List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, addresses);
			RiakCluster cluster = new RiakCluster.Builder(nodes).build();
			cluster.start();
			riakClient = new RiakClient(cluster);
		} catch (UnknownHostException e) {
			log.error("Could not initialize RiakClient: UnknowHostException -> " + e.getMessage());
		}
		
		
		if (log.isDebugEnabled()) {
			log.debug("RiakClient prepared: " + riakClient.toString());
		}

		if (getThreadContext().getVariables().getObject(getConnectionId()) != null) {
			log.warn(getConnectionId() + " has already initialized!");
		} else {
			if (log.isDebugEnabled())
				log.debug(getConnectionId() + " is being initialized ...");

			getThreadContext().getVariables().putObject(getConnectionId(), riakClient);
			/*try {
				getThreadContext().getVariables().putObject(
								getConnectionId(), riakClient);
				Bucket iovBucket = riakClient.fetchBucket("IOV").execute();
				Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
				Bucket chunkBucket = riakClient.fetchBucket("CHUNK").execute();
				getThreadContext().getVariables().putObject(
						getConnectionId().concat("-IOV"), iovBucket);
				getThreadContext().getVariables().putObject(
						getConnectionId().concat("-PAYLOAD"), plBucket);
				getThreadContext().getVariables().putObject(
						getConnectionId().concat("-CHUNK"), chunkBucket);
			} catch (RiakRetryFailedException e) {
				log.error("Could not fetch buckets from riakClient... Exception: " + e.toString());
			}*/
		}
	}

	public static String getProtocolName() {
		return protocol;
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
		return false;
	}

	public String getConnectionId() {
		return getPropertyAsString(CONNECTION_ID);
	}

	public void setConnectionId(String connectionId) {
		setProperty(CONNECTION_ID, connectionId);
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

	public String getCluster() {
		return getPropertyAsString(CLUSTER);
	}

	public void setCluster(String cluster) {
		setProperty(CLUSTER, cluster);
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

	public String getProtocol() {
		return getPropertyAsString(PROTOCOL);
	}

	public void setProtocol(String protocol) {
		setProperty(PROTOCOL, protocol);
	}

	public String getMaxConnection() {
		return getPropertyAsString(MAX_CONNECTION);
	}

	public void setMaxConnection(String maxConnection) {
		setProperty(MAX_CONNECTION, maxConnection);
	} 

	public String getTimeout() {
		return getPropertyAsString(TIMEOUT);
	}

	public void setTimeout(String timeout) {
		setProperty(TIMEOUT, timeout);
	}

}
