package hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;


public class HBaseConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 5124107244481246716L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The ID of the JDBC Connection object. */
	public final static String CONNECTION_ID = "HBaseConfigElement.connectionId";
	public final static String CLUSTER_NAME = "HBaseConfigElement.clusterName";
	public final static String TABLE_LIST = "HBaseConfigElement.tableList";
	public final static String MASTER_HOST = "HBaseConfigElement.masterHost";
	public final static String MASTER_PORT = "HBaseConfigElement.masterPort";
	public final static String ZOOKEEPER_QUORUM = "HbaseConfigElement.zookeeperQuorum";
	public final static String ZOOKEEPER_CLIENT_PORT = "HBaseConfigElement.zookeeperClientPort";
	public final static String MAX_KV_SIZE = "HBaseConfigElement.maxKvSize";


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

	@SuppressWarnings("unchecked")
	public static HTable getHTable(String clusterId, String tableName)
			throws CustomSamplersException {
		Object hTables = JMeterContextService.getContext().getVariables()
				.getObject(clusterId.concat("-HTables"));
		HashMap<String, HTable> hTableMap = null;
		if (hTables == null) {
			throw new CustomSamplersException("HBase HTables object is null!");
		} else {
			if (hTables instanceof HashMap<?, ?>) {
				hTableMap = (HashMap<String, HTable>)hTables;
			}
			else {
				throw new CustomSamplersException("Casting the object to HashMap<String, HTable> failed!");
			}
		}
		HTable ht = hTableMap.get(tableName);
		if (ht != null) {
			return ht;
		} else {
			throw new CustomSamplersException("HTable not found in " + clusterId + "-HTables object!");
		}
	}

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
		}

		Configuration hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum", getZookeeperQuorum());
		hbaseConfig.set("hbase.zookeeper.property.clientPort", getZookeeperClientPort());
		hbaseConfig.set("hbase.master", getMasterHost().concat(":").concat(getMasterPort()));
		hbaseConfig.set("hbase.client.keyvalue.maxsize", getMaxKvSize());

		String tableNames[] = getTableList().replaceAll("^[,\\s]+", "").split("[,\\s]+");
		Map<String, HTable> hTables = new HashMap<String, HTable>();
		for (int i = 0; i < tableNames.length; ++i) {
			try {
				HTable ht = new HTable(hbaseConfig, tableNames[i]);
				hTables.put(tableNames[i], ht);
			} catch (IOException e) {
				log.error("IOException occured while fetching table " + tableNames[i] + " from HBase!");
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Hadoop Configuration: " + hbaseConfig.toString());
			log.debug("HTables:" + hTables.toString());
		}

		JMeterVariables jMeterVars = getThreadContext().getVariables();
		if (jMeterVars.getObject(getConnectionId()) != null 
				|| jMeterVars.getObject(getConnectionId().concat("-HTables")) != null ) {
			if (log.isWarnEnabled()) {
				log.warn(getConnectionId() + " objects are already defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getConnectionId() + " objects are being defined...");
			}
			// Put the HBase Configuration element into the ThreadContext.
			jMeterVars.putObject(getConnectionId(), hbaseConfig);
			// Put the HBase HTable elements into the ThreadContext.
			jMeterVars.putObject(getConnectionId().concat("-HTables"), hTables);

		}

	}

	@Override
	public void testStarted(String arg0) {
		testStarted();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}
		getThreadContext().getVariables().putObject(getConnectionId(), null);

		Object hTables = JMeterContextService.getContext().getVariables()
				.getObject(getConnectionId().concat("-HTables"));
		HashMap<String, HTable> hTableMap = null;
		if (hTables == null) {
			log.error("HBase HTables object is null!");
		}
		else {
			if (hTables instanceof HashMap<?, ?>) {
				hTableMap = (HashMap<String, HTable>)hTables;
				Iterator<Entry<String, HTable>> it = hTableMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, HTable> tableEntry = (Map.Entry<String, HTable>)it.next();
					HTable t = (HTable)tableEntry.getValue();
					try {
						t.close();
					} catch (IOException e) {
						log.error("IOException occured while closing HTable " + tableEntry.getKey());
					}
				}
			}
			else {
				log.error("Casting the object to HashMap<String, HTable> failed!");
			}
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

	public String getConnectionId() {
		return getPropertyAsString(CONNECTION_ID);
	}

	public void setConnectionId(String connectionId) {
		setProperty(CONNECTION_ID, connectionId);
	}

	public String getClusterName() {
		return getPropertyAsString(CLUSTER_NAME);
	}
	public void setClusterName(String clusterName) {
		setProperty(CLUSTER_NAME, clusterName);
	}

	public String getTableList() {
		return getPropertyAsString(TABLE_LIST);
	}
	public void setTableList(String tableList) {
		setProperty(TABLE_LIST, tableList);
	}

	public String getMasterHost() {
		return getPropertyAsString(MASTER_HOST);
	}
	public void setMasterHost(String masterHost) {
		setProperty(MASTER_HOST, masterHost);
	}

	public String getMasterPort() {
		return getPropertyAsString(MASTER_PORT);
	}
	public void setMasterPort(String masterPort) {
		setProperty(MASTER_PORT, masterPort);
	}

	public String getZookeeperQuorum() {
		return getPropertyAsString(ZOOKEEPER_QUORUM);
	}
	public void setZookeeperQuorum(String zookeeperQuorum) {
		setProperty(ZOOKEEPER_QUORUM, zookeeperQuorum);
	}

	public String getZookeeperClientPort() {
		return getPropertyAsString(ZOOKEEPER_CLIENT_PORT);
	}
	public void setZookeeperClientPort(String zookeeperClientPort) {
		setProperty(ZOOKEEPER_CLIENT_PORT, zookeeperClientPort);
	}

	public String getMaxKvSize() {
		return getPropertyAsString(MAX_KV_SIZE);
	}
	public void setMaxKvSize(String maxKvSize) {
		setProperty(MAX_KV_SIZE, maxKvSize);
	}

}
