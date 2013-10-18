package accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;


public class AccumuloConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 6226522728865127060L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CLUSTERID = "AccumuloConfigElement.clusterid";
	public final static String INSTANCE = "AccumuloConfigElement.instance";
	public final static String ZOOKEEPERHOST = "AccumuloConfigElement.zookeeperHost";
	public final static String ZOOKEEPERPORT = "AccumuloConfigElement.zookeeperPort";
	public final static String USERNAME = "AccumuloConfigElement.username";
	public final static String PASSWORD = "AccumuloConfigElement.password";

	public static Connector getAccumuloConnector(String clusterId)
			throws CustomSamplersException {
		Object connector = JMeterContextService.getContext().getVariables().getObject(clusterId);
		if (connector == null) {
			throw new CustomSamplersException("Accumulo's Connector object is null!");
		}
		else {
			if (connector instanceof Connector) {
				return (Connector)connector;
			}
			else {
				throw new CustomSamplersException("Casting the object to (Accumulo) Connector failed!");
			}
		}
	}

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
		}

		Instance zookeeper = new ZooKeeperInstance(getInstance(),
				getZookeeperHost().concat(":").concat(getZookeeperPort()));
		Connector accumulo = null;
		try {
			accumulo = zookeeper.getConnector(getUsername(), getPassword().getBytes());
		} catch (AccumuloException e) {
			log.error("AccumuloException occured! Details: " + e.toString());
		} catch (AccumuloSecurityException e) {
			log.error("AccumuloSecurityException occured! Details: " + e.toString());
		}

		if (log.isDebugEnabled()) {
			log.debug("Zookeeper speaks?: " + zookeeper.getInstanceID());
			log.debug("Accumulo speaks?: " + accumulo.whoami());
		}

		JMeterVariables jMeterVars = getThreadContext().getVariables();
		if (jMeterVars.getObject(getClusterId()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getClusterId() + " objects are already defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getClusterId() + " objects are being defined...");
			}
			// Put the Accumulo's Zookeeper element into the ThreadContext.
			jMeterVars.putObject(getClusterId().concat("-ZK"), zookeeper);
			// Put the Accumulo Connector elements into the ThreadContext.
			jMeterVars.putObject(getClusterId(), accumulo);
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

		JMeterVariables vars = getThreadContext().getVariables();

		Object zookeeper = vars.getObject(getClusterId().concat("-ZK"));
		Object accumulo = vars.getObject(getClusterId());

		if (zookeeper == null || accumulo == null) {
			log.error("Some of the Accumulo objects are null!");
		} else {
			vars.putObject(getClusterId().concat("-ZK"), null);
			vars.putObject(getClusterId(), null);
		}
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

	public String getClusterId() {
		return getPropertyAsString(CLUSTERID);
	}
	public void setClusterId(String clusterId) {
		setProperty(CLUSTERID, clusterId);
	}

	public String getInstance() {
		return getPropertyAsString(INSTANCE);
	}
	public void setInstance(String instance) {
		setProperty(INSTANCE, instance);
	}

	public String getZookeeperHost() {
		return getPropertyAsString(ZOOKEEPERHOST);
	}
	public void setZookeeperHost(String zookeeperHost) {
		setProperty(ZOOKEEPERHOST, zookeeperHost);
	}

	public String getZookeeperPort() {
		return getPropertyAsString(ZOOKEEPERPORT);
	}
	public void setZookeeperPort(String zookeeperPort) {
		setProperty(ZOOKEEPERPORT, zookeeperPort);
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

}
