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

	public final static String CLUSTERID = "HBaseConfigElement.clusterid";
	public final static String TABLELIST = "HBaseConfigElement.tableList";
	public final static String MASTERHOST = "HBaseConfigElement.masterHost";
	public final static String MASTERPORT = "HBaseConfigElement.masterPort";
	public final static String ZOOKEEPERQUORUM = "HbaseConfigElement.zookeeperQuorum";
	public final static String ZOOKEEPERCLIENTPORT = "HBaseConfigElement.zookeeperClientPort";
	public final static String MAXKVSIZE = "HBaseConfigElement.maxKvSize";


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
		}
		else {
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
		if (jMeterVars.getObject(getClusterId()) != null 
				|| jMeterVars.getObject(getClusterId().concat("-HTables")) != null ) {
			if (log.isWarnEnabled()) {
				log.warn(getClusterId() + " objects are already defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getClusterId() + " objects are being defined...");
			}
			// Put the HBase Configuration element into the ThreadContext.
			jMeterVars.putObject(getClusterId(), hbaseConfig);
			// Put the HBase HTable elements into the ThreadContext.
			jMeterVars.putObject(getClusterId().concat("-HTables"), hTables);

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
		getThreadContext().getVariables().putObject(getClusterId(), null);

		Object hTables = JMeterContextService.getContext().getVariables()
				.getObject(getClusterId().concat("-HTables"));
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

	public String getClusterId() {
		return getPropertyAsString(CLUSTERID);
	}
	public void setClusterId(String clusterId) {
		setProperty(CLUSTERID, clusterId);
	}

	public String getTableList() {
		return getPropertyAsString(TABLELIST);
	}
	public void setTableList(String tableList) {
		setProperty(TABLELIST, tableList);
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

	public String getMaxKvSize() {
		return getPropertyAsString(MAXKVSIZE);
	}
	public void setMaxKvSize(String maxKvSize) {
		setProperty(MAXKVSIZE, maxKvSize);
	}

}
