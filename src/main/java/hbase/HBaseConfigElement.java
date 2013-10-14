package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;


public class HBaseConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 5124107244481246716L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CLUSTERID = "HBaseConfigElement.clusterid";
	public final static String MASTERHOST = "HBaseConfigElement.masterHost";
	public final static String MASTERPORT = "HBaseConfigElement.masterPort";
	
	public final static String ZOOKEEPERQUORUM = "HbaseConfigElement.zookeeperQuorum";
	public final static String ZOOKEEPERCLIENTPORT = "HBaseConfigElement.zookeeperClientPort";


	public static Configuration getHBaseConfiguration(String clusterId)
			throws CustomSamplersException {
		Object hbaseConfig = JMeterContextService.getContext().getVariables().getObject(clusterId);
		if (hbaseConfig == null) {
			throw new CustomSamplersException("HBase Configuration object is null!");
		}
		else {
			if (hbaseConfig instanceof Configuration) {
				return (Configuration)hbaseConfig;
			}
			else {
				throw new CustomSamplersException("Casting the object to Configuration failed!");
			}
		}
	}

	@SuppressWarnings({ "static-access", "deprecation" })
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
		}

		Configuration hbaseConf = new HBaseConfiguration().create();
		hbaseConf.set("hbase.zookeeper.quorum", getZookeeperQuorum());
		hbaseConf.set("hbase.zookeeper.property.clientPort", getZookeeperClientPort());
		hbaseConf.set("hbase.master", getMasterHost().concat(":").concat(getMasterPort()));

		if (log.isDebugEnabled()) {
			log.debug("Hadoop Configuration: " + hbaseConf.toString());
		}

		if (getThreadContext().getVariables().getObject(getClusterId()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getClusterId() + " has already been defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getClusterId() + " is being defined...");
			}
			// Put the HBase Configuration element into the ThreadContext.
			getThreadContext().getVariables().putObject(getClusterId(), hbaseConf);

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
		
		getThreadContext().getVariables().putObject(getClusterId(), null);
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

	public String getMasterHost() {
		return getPropertyAsString(MASTERHOST);
	}
	public void setMasterHost(String masterHost) {
		setProperty(MASTERHOST, masterHost);
	}

	public String getMasterPort() {
		return getPropertyAsString(MASTERPORT);
	}
	public void setMasterPort(String masterPort) {
		setProperty(MASTERPORT, masterPort);
	}

	public String getZookeeperQuorum() {
		return getPropertyAsString(ZOOKEEPERQUORUM);
	}
	public void setZookeeperQuorum(String zookeeperQuorum) {
		setProperty(ZOOKEEPERQUORUM, zookeeperQuorum);
	}

	public String getZookeeperClientPort() {
		return getPropertyAsString(ZOOKEEPERCLIENTPORT);
	}
	public void setZookeeperClientPort(String zookeeperClientPort) {
		setProperty(ZOOKEEPERCLIENTPORT, zookeeperClientPort);
	}

}
