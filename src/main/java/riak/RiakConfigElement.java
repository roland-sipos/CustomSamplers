package riak;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

import exceptions.CustomSamplersException;

/**
 * 
 * */
public class RiakConfigElement extends AbstractTestElement
	implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -4956133291185363007L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String HOST = "RiakConfigElement.host";
	public final static String PORT = "RiakConfigElement.port";
	public final static String CLUSTER = "RiakConfigElement.cluster";
	public final static String USERNAME = "RiakConfigElement.username";
	public final static String PASSWORD = "RiakConfigElement.password";

	public final static String PROTOCOL = "RiakConfigElement.protocol";
	public final static String MAX_CONNECTION = "RiakConfigElement.maxConnection";
	public final static String TIMEOUT = "RiakConfigElement.timeout";
	
	@Override
	public void testEnded() {
		IRiakClient riak = null;
		try {
			riak = getRiakClient(getCluster());
		} catch (CustomSamplersException e) {
			log.error("Unable to get the RiakClient for Shutdown in testEnd()!");
		} finally {
			riak.shutdown();
		}
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static IRiakClient getRiakClient(String cluster) throws CustomSamplersException {
		Object riak = JMeterContextService.getContext().getVariables().getObject(cluster);
		if (riak == null) {
			throw new CustomSamplersException("RiakClient object is null!");
		}
		else {
			if (riak instanceof IRiakClient) {
				return (IRiakClient)riak;
			}
			else {
				throw new CustomSamplersException("Casting the object to IRiakClient failed!");
			}
		}
	}
	
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}
		
		String[] hosts = getHost().split(":");
		IRiakClient riakClient = null;
		if (getProtocol().equals("HTTP")) {
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
			PBClientConfig.Builder configBuilder = 
					PBClientConfig.Builder.from(PBClientConfig.defaults());
			configBuilder.withHost(hosts[0]);
			configBuilder.withPort(Integer.parseInt(getPort()));
			configBuilder.withRequestTimeoutMillis(Integer.parseInt(getTimeout()));
			configBuilder.build();
			
			PBClientConfig clientConfig = configBuilder.build();
			PBClusterConfig clusterConf = new PBClusterConfig(Integer.parseInt(getMaxConnection()));
			clusterConf.addClient(clientConfig);
			for (int i = 0; i < hosts.length; ++i) {
				System.out.println("Adding host to HTTP cluster config: " + hosts[i]);
				clusterConf.addHosts(hosts[i]);
			}
			
			try {
				riakClient = RiakFactory.newClient(clusterConf);
			} catch (RiakException e) {
				log.error("RiakFactory was unabled to create a new client " 
			              + "based on the PBClusterConfig: " + e.toString());
			}
			
		} else {
			log.error("Unknown protocol requested for the connection builder!");
		}
			
		if (log.isDebugEnabled()) {
			log.debug("RiakClient prepared: " + riakClient.toString());
		}
		
		if (getThreadContext().getVariables().getObject(getCluster()) != null) {
			log.warn(getCluster() + " has already initialized!");
		} else {
			if (log.isDebugEnabled())
				log.debug(getCluster() + " is being initialized ...");

 			getThreadContext().getVariables().putObject(getCluster(), riakClient);
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
		return false;
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
