package mongodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.gridfs.GridFS;

import utils.CustomSamplerUtils;

public class MongoConnectionSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -9047124734341672662L;
	/** Static logger from the LoggingManager. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The ID of the MongoDB DB object. */
	public final static String CONNECTIONID = "MongoConfigElement.connectionId";
	/** The host name of connection target. */
	public final static String HOST = "MongoConfigElement.host";
	/** The port to be used by the connection. */
	public final static String PORT = "MongoConfigElement.port";
	/** The name of the database instance on target host. */
	public final static String DATABASE = "MongoConfigElement.database";
	/** User name for authentication. */
	public final static String USERNAME = "MongoConfigElement.username";
	/** Password for authentication. */
	public final static String PASSWORD = "MongoConfigElement.password";
	/** If the connection should auto-retry to establish the connection. */
	public final static String AUTO_CONNECT_RETRY = "MongoConfigElement.autoConnectRetry";
	/** The maximum allowed connection through this object. */
	public final static String CONNECTIONS_PER_HOST = "MongoConfigElement.connectionsPerHost";
	/** The timeout of the connection object. */
	public final static String CONNECT_TIMEOUT = "MongoConfigElement.connectTimeout";
	/** The maximum time of retry establishing connection. */
	public final static String MAX_AUTO_CONNECT_RETRY_TIME = "MongoConfigElement.maxAutoConnectRetryTime";
	/** The maximum idle time on this connection object. */
	public final static String MAX_WAIT_TIME = "MongoConfigElement.maxWaitTime";
	/** The timeout of the socket that is used by this connection. */
	public final static String SOCKET_TIMEOUT = "MongoConfigElement.socketTimeout";
	/** If the connection object should keep the socket alive. */
	public final static String SOCKET_KEEP_ALIVE = "MongoConfigElement.socketKeepAlive";
	/** The thread block multiplier. */
	public final static String THREADS_ALLOWED_TO_BLOCK_MULTIPLIER = "MongoConfigElement.threadsAllowedToBlockMultiplier";


	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getName());
		try {
			if (getThreadContext().getVariables().getObject(getConnectionId()) != null ) {
				res.sampleStart();
				DB db = MongoConfigElement.getMongoDB(getConnectionId());
				System.out.println("-> Closing db: " + db.toString());
				System.out.println("-> Closing client: " + db.getMongo().toString());
				db.getMongo().close();
			} else {
				res.sampleStart();
				createAndStore();
			}
		} catch (Exception e) {
			CustomSamplerUtils.finalizeResponse(res, false, "999",
					"Exception occured in this sample: " + e.toString());
		} finally {
			CustomSamplerUtils.finalizeResponse(res, true, "200", "JDBC Object is placed successfully, "
					+ "with ID: " + getConnectionId());
			res.sampleEnd();
		}
		return res;
	}

	private void createAndStore() {
		MongoClientOptions.Builder mongoConf = null;
		try {
			mongoConf = new Builder();
		} catch (Exception e) {
			log.error("Failed to create MongoClientOptions... " + e);
		}

		try {
			mongoConf.autoConnectRetry(Boolean.parseBoolean(getAutoConnectRetry()));
			mongoConf.connectionsPerHost(Integer.parseInt(getConnectionsPerHost()));
			mongoConf.connectTimeout(Integer.parseInt(getConnectTimeout()));
			mongoConf.maxAutoConnectRetryTime(Integer.parseInt(getMaxAutoConnectRetryTime()));
			mongoConf.maxWaitTime(Integer.parseInt(getMaxWaitTime()));
			mongoConf.socketTimeout(Integer.parseInt(getSocketTimeout()));
			mongoConf.socketKeepAlive(Boolean.parseBoolean(getSocketKeepAlive()));
			mongoConf.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(getThreadsAllowedToBlockMultiplier()));
			mongoConf.readPreference(ReadPreference.nearest());
		} catch (NumberFormatException e) {
			log.error("Some of the config value parsing was failed: " + e);
		}

		MongoClient mongoClient = null;
		try {
			if (getHost().contains(",")) {
				Integer port = Integer.parseInt(getPort());
				String hosts[] = getHost().split(",");
				List<ServerAddress> addresses = new ArrayList<ServerAddress>();
				for (int i = 0; i < hosts.length; ++i) {
					addresses.add(new ServerAddress(hosts[i], port));
				}
				mongoClient = new MongoClient(addresses, mongoConf.build());
			} else {
				ServerAddress address = new ServerAddress(getHost(), Integer.parseInt(getPort()));
				mongoClient = new MongoClient(address, mongoConf.build());
				System.out.println("-> New client: " + mongoClient.toString());
			}
		} catch (Exception e) {
			log.error("MongoClient initialization failed due to: " + e.toString());
		}

		DB mongoDB = mongoClient.getDB(getDatabase());
		System.out.println("-> New DB: " + mongoDB.toString());
		GridFS gridFS = new GridFS(mongoDB, "PAYLOAD");
		System.out.println("-> New GridFS: " + gridFS.toString());
		boolean auth = mongoDB.isAuthenticated();
		if (!auth) {
			if (!getUsername().equals("")) {
				auth = mongoDB.authenticate(getUsername(), getPassword().toCharArray());
				if (!auth) {
					log.error("MongoClient authentication failed...");
				}
			} else {
				log.error("MongoClient username is empty, but authentication is needed!");
			}
		}
		getThreadContext().getVariables().putObject(getConnectionId(), mongoDB);
		getThreadContext().getVariables().putObject(getConnectionId().concat("-GFS"), gridFS);
	}
	

	public String getConnectionId() {
		return getPropertyAsString(CONNECTIONID);
	}

	public void setConnectionId(String connectionId) {
		setProperty(CONNECTIONID, connectionId);
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

	public String getAutoConnectRetry() {
		return getPropertyAsString(AUTO_CONNECT_RETRY);
	}

	public void setAutoConnectRetry(String autoConnectRetry) {
		setProperty(AUTO_CONNECT_RETRY, autoConnectRetry);
	}

	public String getConnectionsPerHost() {
		return getPropertyAsString(CONNECTIONS_PER_HOST);
	}

	public void setConnectionsPerHost(String connectionsPerHost) {
		setProperty(CONNECTIONS_PER_HOST, connectionsPerHost);
	}

	public String getConnectTimeout() {
		return getPropertyAsString(CONNECT_TIMEOUT);
	}

	public void setConnectTimeout(String connectTimeout) {
		setProperty(CONNECT_TIMEOUT, connectTimeout);
	}

	public String getMaxAutoConnectRetryTime() {
		return getPropertyAsString(MAX_AUTO_CONNECT_RETRY_TIME);
	}

	public void setMaxAutoConnectRetryTime(String maxAutoConnectRetryTime) {
		setProperty(MAX_AUTO_CONNECT_RETRY_TIME, maxAutoConnectRetryTime);
	}

	public String getMaxWaitTime() {
		return getPropertyAsString(MAX_WAIT_TIME);
	}

	public void setMaxWaitTime(String maxWaitTime) {
		setProperty(MAX_WAIT_TIME, maxWaitTime);
	}

	public String getSocketTimeout() {
		return getPropertyAsString(SOCKET_TIMEOUT);
	}

	public void setSocketTimeout(String socketTimeout) {
		setProperty(SOCKET_TIMEOUT, socketTimeout);
	}

	public String getSocketKeepAlive() {
		return getPropertyAsString(SOCKET_KEEP_ALIVE);
	}

	public void setSocketKeepAlive(String socketKeepAlive) {
		setProperty(SOCKET_KEEP_ALIVE, socketKeepAlive);
	}

	public String getThreadsAllowedToBlockMultiplier() {
		return getPropertyAsString(THREADS_ALLOWED_TO_BLOCK_MULTIPLIER);
	}

	public void setThreadsAllowedToBlockMultiplier(String threadsAllowed) {
		setProperty(THREADS_ALLOWED_TO_BLOCK_MULTIPLIER, threadsAllowed);
	}

}
