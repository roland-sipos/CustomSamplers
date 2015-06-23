package couchdb;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.ektorp.CouchDbConnector;

import utils.CustomSamplerUtils;

/**
 * This class is the Sampler for JDBC Connection creation.
 * The member fields are the user options, set by the appropriate BeanInfo class.
 * */
public class CouchConnectionSampler extends AbstractSampler implements TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = -5887808618625615044L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The ID of the CouchDB Connection object that is managed by this ConfigElement. */
	public final static String CONNECTION_ID = "CouchConfigElement.connectionId";
	public final static String HOST = "CouchConfigElement.host";
	public final static String PORT = "CouchConfigElement.port";
	public final static String DATABASE = "CouchConfigElement.database";
	public final static String USERNAME = "CouchConfigElement.username";
	public final static String PASSWORD = "CouchConfigElement.password";
	public final static String CREATE_IF_NOT_EXISTS = "CouchConfigElement.createIfNotExists";

	public final static String MAX_CONNECTIONS = "CouchConfigElement.maxConnections";
	public final static String CONNECTION_TIMEOUT = "CouchConfigElement.connectionTimeout";
	public final static String SOCKET_TIMEOUT = "CouchConfigElement.socketTimeout";
	public final static String CACHING = "CouchConfigElement.caching";
	public final static String MAX_CACHE_ENTRIES = "CouchConfigElement.maxCacheEntries";
	public final static String MAX_OBJECT_SIZE_BYTES = "CouchConfigElement.maxObjectSizeBytes";
	public final static String USE_EXPECT_CONTINUE = "CouchConfigElement.useExpectContinue";
	public final static String CLEANUP_IDLE_CONNECTIONS = "CouchConfigElement.cleanupIdleConnections";

	public CouchConnectionSampler() {
		trace("CouchConnectionSampler()" + this.toString());
	}

	/**
	 * The sample function is called by every thread that are defined in the current
	 * JMeter ThreadGroup. It creates a CouchDB Connection and place it to the ThreadGroup's
	 * current Thread's context as a variable. (So it's unique for every thread.) The
	 * returned SampleResult contains the measured time to create the connection.
	 * 
	 * @param  arg0  the Entry for this sample
	 * @return  SampleResult  the result of the sample
	 * */
	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getName());
		try {
			if (getThreadContext().getVariables().getObject(getConnectionId()) != null ) {
				res.sampleStart();
				CouchDbConnector couchDB = CouchConfigElement.getCouchDB(getConnectionId());
				couchDB.getConnection().shutdown();
				res.sampleEnd();
				CustomSamplerUtils.finalizeResponse(res, true, "200", "CouchDbConnector connection closed."
						+ "with ID: " + getConnectionId());
				return res;
			} else {
				res.sampleStart();
				CouchDbConnector couchDB = CouchConfigElement.createCouchConnection(getDatabase(),
						getHost(), getPort(), getMaxConnections(), getConnectionTimeout(),
						getSocketTimeout(), getCaching(), getMaxCacheEntries(), getMaxObjectSizeBytes(),
						getUseExpectContinue(), getCleanupIdleConnections(), getCreateIfNotExists(),
						getUsername(), getPassword());
				res.samplePause();
				getThreadContext().getVariables().putObject(getConnectionId(), couchDB);
			}
		} catch (Exception e) {
			CustomSamplerUtils.finalizeResponse(res, false, "999",
					"Exception occured in this sample: " + e.toString());
		} finally {
			CustomSamplerUtils.finalizeResponse(res, true, "200", "JDBC Object is placed successfully, "
					+ "with ID: " + getDatabase());
			res.sampleEnd();
		}
		return res;
	}

	/**
	 * Utility function for logging in the Sampler.
	 * @param  s  trace message
	 * */
	private void trace(String s) {
		if(log.isDebugEnabled()) {
			log.debug(Thread.currentThread().getName() + " (" + getTitle()
					+ " " + s + " " + this.toString());
		}
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

	public String getCreateIfNotExists() {
		return getPropertyAsString(CREATE_IF_NOT_EXISTS);
	}

	public void setCreateIfNotExists(String createIfNotExists) {
		setProperty(CREATE_IF_NOT_EXISTS, createIfNotExists);
	}

	public String getMaxConnections() {
		return getPropertyAsString(MAX_CONNECTIONS);
	}

	public void setMaxConnections(String maxConnections) {
		setProperty(MAX_CONNECTIONS, maxConnections);
	}

	public String getConnectionTimeout() {
		return getPropertyAsString(CONNECTION_TIMEOUT);
	}

	public void setConnectionTimeout(String connectionTimeout) {
		setProperty(CONNECTION_TIMEOUT, connectionTimeout);
	}

	public String getSocketTimeout() {
		return getPropertyAsString(SOCKET_TIMEOUT);
	}

	public void setSocketTimeout(String socketTimeout) {
		setProperty(SOCKET_TIMEOUT, socketTimeout);
	}

	public String getCaching() {
		return getPropertyAsString(CACHING);
	}

	public void setCaching(String caching) {
		setProperty(CACHING, caching);
	}

	public String getMaxCacheEntries() {
		return getPropertyAsString(MAX_CACHE_ENTRIES);
	}

	public void setMaxCacheEntries(String maxCacheEntries) {
		setProperty(MAX_CACHE_ENTRIES, maxCacheEntries);
	}

	public String getMaxObjectSizeBytes() {
		return getPropertyAsString(MAX_OBJECT_SIZE_BYTES);
	}

	public void setMaxObjectSizeBytes(String maxObjectSizeBytes) {
		setProperty(MAX_OBJECT_SIZE_BYTES, maxObjectSizeBytes);
	}

	public String getUseExpectContinue() {
		return getPropertyAsString(USE_EXPECT_CONTINUE);
	}

	public void setUseExpectContinue(String useExpectContinue) {
		setProperty(USE_EXPECT_CONTINUE, useExpectContinue);
	}

	public String getCleanupIdleConnections() {
		return getPropertyAsString(CLEANUP_IDLE_CONNECTIONS);
	}

	public void setCleanupIdleConnections(String cleanupIdleConnections) {
		setProperty(CLEANUP_IDLE_CONNECTIONS, cleanupIdleConnections);
	}

}
