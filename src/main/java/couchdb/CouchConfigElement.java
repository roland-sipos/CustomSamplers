package couchdb;

import java.net.MalformedURLException;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import utils.CustomSamplersException;


public class CouchConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -6669728766687401677L;
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

	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}
		try {
			CouchDbConnector couchDB = getCouchDB(getConnectionId());
			couchDB.getConnection().shutdown();
		} catch (CustomSamplersException e) {
			log.error("CouchConfigElement.testEnded() -> "
					+ "Could not fetch CouchDbConnector with id: " + getConnectionId());
			e.printStackTrace();
		}
		getThreadContext().getVariables().putObject(getConnectionId(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static CouchDbConnector getCouchDB(String connectionId) throws CustomSamplersException {
		Object couch = JMeterContextService.getContext().getVariables().getObject(connectionId);
		if (couch == null) {
			throw new CustomSamplersException("CouchDB object is null!");
		}
		else {
			if (couch instanceof CouchDbConnector) {
				return (CouchDbConnector)couch;
			}
			else {
				throw new CustomSamplersException("Casting the object to CouchDBConnector failed!");
			}
		}
	}

	/**
	 * A static function for creating CouchDbConnectors, based on given parameters.
	 * 
	 * @param database the CouchDB database name
	 * 
	 * @return CouchDbConnector the set up CouchDB Connection
	 * */
	public static CouchDbConnector createCouchConnection(String database, String host, String port,
			String maxConnections, String connectionTimeout, String socketTimeout, String caching,
			String maxCacheEntries, String maxObjectSizeBytes, String useExpectContinue,
			String cleanupIdleConnections, String createIfNotExists, String username, String password) {
		CouchDbConnector couchDB = null;
		try {
			StdHttpClient.Builder builder = new StdHttpClient.Builder()
				.url(host.concat(":").concat(port))
				.maxConnections(Integer.valueOf(maxConnections))
				.connectionTimeout(Integer.valueOf(connectionTimeout))
				.socketTimeout(Integer.valueOf(socketTimeout))
				.caching(Boolean.valueOf(caching))
				.maxCacheEntries(Integer.valueOf(maxCacheEntries))
				.maxObjectSizeBytes(Integer.valueOf(maxObjectSizeBytes))
				.useExpectContinue(Boolean.valueOf(useExpectContinue))
				.cleanupIdleConnections(Boolean.valueOf(cleanupIdleConnections));
			if (username.equals("")) {
				builder.username(username).password(password);
			}
			HttpClient client = builder.build();
			StdCouchDbInstance dbInstance = new StdCouchDbInstance(client);
			couchDB = dbInstance.createConnector(database,
					Boolean.parseBoolean(createIfNotExists));
		} catch (MalformedURLException e) {
			log.error("MalformedURLException occured when creating CouchDB Instance...");
			e.printStackTrace();
		}
		return couchDB;
	}

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}
		
		if (getThreadContext().getVariables().getObject(getConnectionId()) != null) {
			log.warn(getConnectionId() + " has already initialized!");
		} else {
			if (log.isDebugEnabled()) {
				log.debug(getConnectionId() + " is being initialized ...");
			}

			CouchDbConnector couchDB = createCouchConnection(getDatabase(), getHost(), getPort(),
					getMaxConnections(), getConnectionTimeout(), getSocketTimeout(), getCaching(),
					getMaxCacheEntries(), getMaxObjectSizeBytes(), getUseExpectContinue(),
					getCleanupIdleConnections(), getCreateIfNotExists(), getUsername(), getPassword());

			getThreadContext().getVariables().putObject(getConnectionId(), couchDB);
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
