package hbase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Callback;

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


	public static HBaseClient getHBaseClient(String connectionId)
			throws CustomSamplersException {
		Object hbaseClient = JMeterContextService.getContext().getVariables().getObject(connectionId);
		if (hbaseClient == null) {
			throw new CustomSamplersException("HBaseClient object is null!");
		}
		else {
			if (hbaseClient instanceof HBaseClient) {
				return (HBaseClient)hbaseClient;
			}
			else {
				throw new CustomSamplersException("Casting the object to HBaseClient failed!");
			}
		}
	}

	/*@SuppressWarnings("unchecked")
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
	}*/

	private boolean ensureTableExist(final HBaseClient client, final String table) {
		final CountDownLatch plLatch = new CountDownLatch(1);
		final AtomicBoolean plFail= new AtomicBoolean(false);
		client.ensureTableExists(table).addCallbacks(
				new Callback<Object, Object>() {
					@Override
					public Object call(Object arg) throws Exception {
						plLatch.countDown();
						return null;
					}
				},
				new Callback<Object, Object>() {
					@Override
					public Object call(Object arg) throws Exception {
						plFail.set(true);
						plLatch.countDown();
						return null;
					}
				}
		);

		try {
			plLatch.await();
		} catch (InterruptedException e) {
			System.out.println("Exception for waiting for table lookup: " + e.toString());
		}
		if (plFail.get()) {
			System.out.println("Could not find PAYLOAD table.");
			return false;
		}
		return true;
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

		HBaseClient hbaseClient = new HBaseClient(ZKConfig.getZKQuorumServersString(hbaseConfig));
		final boolean plExists = ensureTableExist(hbaseClient, "PAYLOAD");
		final boolean iovExists = ensureTableExist(hbaseClient, "IOV");
		
		if (!plExists || !iovExists) {
			log.error("Missing tables in HBase! Payload:" + plExists + " IOV:" + iovExists);
		}
		
		/*String tableNames[] = getTableList().replaceAll("^[,\\s]+", "").split("[,\\s]+");
		Map<String, HTable> hTables = new HashMap<String, HTable>();
		for (int i = 0; i < tableNames.length; ++i) {
			try {
				HTable ht = new HTable(hbaseConfig, tableNames[i]);
				hTables.put(tableNames[i], ht);
			} catch (IOException e) {
				log.error("IOException occured while fetching table " + tableNames[i] + " from HBase!");
			}
		}*/

		if (log.isDebugEnabled()) {
			log.debug("Hadoop Configuration: " + hbaseConfig.toString());
			log.debug("HBaseClient: " + hbaseClient.toString());
			//log.debug("HTables:" + hTables.toString());
		}

		JMeterVariables jMeterVars = getThreadContext().getVariables();
		if (jMeterVars.getObject(getConnectionId()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getConnectionId() + " objects are already defined! Will not replace!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getConnectionId() + " objects are being defined...");
			}
		
			// Put the HBase Configuration element into the ThreadContext.
			jMeterVars.putObject(getConnectionId(), hbaseClient);
			// Put the HBase HTable elements into the ThreadContext.
			//jMeterVars.putObject(getConnectionId().concat("-HTables"), hTables);
		}
		
		
		/*if (jMeterVars.getObject(getConnectionId()) != null 
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

		}*/

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
		try {
			HBaseClient client = getHBaseClient(getConnectionId());
			client.shutdown();
		} catch (CustomSamplersException e) {
			log.error("Could not fetch and close HBaseClient with id: " + getConnectionId());
		}
		
		getThreadContext().getVariables().putObject(getConnectionId(), null);
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
