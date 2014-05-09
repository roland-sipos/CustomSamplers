package customjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;

/**
 * A Custom ConfigElement for handling JDBC connections.
 * */
public class CustomJDBCConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	/** Generated serialVersionUID. */
	private static final long serialVersionUID = 6256939089220907405L;
	/** Static logger from the LoggingManager. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The ID of the JDBC Connection object. */
	public final static String CONNECTION_ID = "CustomJDBCConfigElement.connectionId";
	/** The JDBC name of the database. */
	public final static String JDBC_NAME = "CustomJDBCConfigElement.jdbcName";
	/** The class name of the JDBC driver. */
	public final static String CLASS_NAME = "CustomJDBCConfigElement.className";
	/** The host name of connection target. */
	public final static String HOST = "CustomJDBCConfigElement.host";
	/** The port to be used by the connection. */
	public final static String PORT = "CustomJDBCConfigElement.port";
	/** The name of the database instance on target host. */
	public final static String DATABASE = "CustomJDBCConfigElement.database";
	/** The SID if the connection should use this instead of Database name. */
	public final static String SID = "CustomJDBCConfigElement.sid";
	/** If the connection should use auto-commit mode or not. */
	public final static String AUTO_COMMIT = "CustomJDBCConfigElement.autoCommit";
	/** If the connection is in read only mode. */
	public final static String READ_ONLY = "CustomJDBCConfigElement.readOnly";
	/** Set the level of transaction isolation. */
	//public final static String TRANSACTION_ISOLATION = "CustomJDBCConfigElement.transactionIsolation";
	/** User name for authentication. */
	public final static String USERNAME = "CustomJDBCConfigElement.username";
	/** Password for authentication. */
	public final static String PASSWORD = "CustomJDBCConfigElement.password";

	/**
	 * The testEnded function of this custom ConfigElement.
	 * It closes the connection in the JMeter context service, with the given ConnectionID.
	 * */
	@Override
	public void testEnded() {
		/** Lookup object with ConnectionID in the JMeter Context. */
		Object connectionObject =
				JMeterContextService.getContext().getVariables().getObject(getConnectionId());
		if (connectionObject == null) {
			log.error("JDBC Connection object is null!");
		}
		else {
			/** Paranoid type check, and closing the instance. */
			if (connectionObject instanceof Connection) {
				Connection conn = (Connection)connectionObject;
				try {
					conn.close();
				} catch (SQLException e) {
					log.error("Could not close the JDBC Connection!");
				}
			}
			else {
				log.error("Casting the object to (java.sql.Connection) failed!");
			}
		}
		getThreadContext().getVariables().putObject(getConnectionId(), null);
	}

	/** 
	 * The remote testEnded matches the normal testEnded function.
	 * */
	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	/**
	 * A static function for creating JDBC Connections, based on given parameters.
	 * 
	 * @param jdbcName the JDBC name
	 * @param host  the target host
	 * @param port  the port of target host
	 * @param sid  SID of the database instance
	 * @param database  alias of the database instance
	 * @param username  user name for authentication
	 * @param password  password of the user for authentication
	 * 
	 * @return Connection the set up JDBC Connection
	 * */
	public static Connection createJDBCConnection(String jdbcName, String host,
			String port, String sid, String database, String username, String password) {
		Connection connection = null;
		String connectionStr = jdbcName + "://" + host + ":" + port + "/" + database;
		if (!sid.isEmpty()) {
			connectionStr = jdbcName + ":@" + host + ":" + port + ":" + sid;
		}
		try {
			connection = DriverManager.getConnection(connectionStr, username, password);
		} catch (SQLException e) {
			log.error("Failed to connect to: " + connectionStr
					+ " Usr/pswd: " + username + "/" + password
					+ " Exception: " + e.toString());
		}
		if (connection != null) {
			log.debug("Connection established for: " + connectionStr);
		}
		return connection;
	}

	/**
	 * Static getter function for looking up JDBC Connection instances in the JMeter Context Service.
	 * 
	 * @param connectionID  the ID of the Connection object to look up
	 * 
	 * @return Connection  the found ConnectionID in 
	 * */
	public static Connection getJDBCConnection(String connectionID) 
			throws CustomSamplersException {
		Object connection = JMeterContextService.getContext().getVariables().getObject(connectionID);
		if (connection == null) {
			throw new CustomSamplersException("JDBC Connection object is null!");
		}
		else {
			if (connection instanceof Connection) {
				return (Connection)connection;
			}
			else {
				throw new CustomSamplersException("Casting the object to (java.sql.Connection) failed!");
			}
		}
	}

	/**
	 * The testStarted function of this custom ConfigElement.
	 * It creates a JDBC Connection, and puts it into the JMeter Context Service Variables
	 * with it's key as the given ConnectionID.
	 * */
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}

		/** Fast Class availability check. */
		try {
			Class.forName(getClassName());
		} catch (ClassNotFoundException e) {
			log.error("JDBC Driver class not found for: " + getClassName() + " Exception:" + e);
		}

		/** Before put, it looks up the Thread Context, if any resource already holds this ID. */
		if (getThreadContext().getVariables().getObject(getConnectionId()) != null) {
			log.warn(getConnectionId() + " has already initialized!");
		} else {
			if (log.isDebugEnabled()) {
				log.debug(getConnectionId() + " is being initialized ...");
			}

			/** If not, it creates the Connection, and put's it into the JMeter Context. */
			Connection connection = createJDBCConnection(getJdbcName(), getHost(),
					getPort(), getSid(), getDatabase(), getUsername(), getPassword());

			try {
				connection.setAutoCommit(Boolean.parseBoolean(getAutoCommit()));
				connection.setReadOnly(Boolean.parseBoolean(getReadOnly()));
			} catch (SQLException e) {
				log.error("Failed to change autoCommit to false: " + e.toString());
			}

			getThreadContext().getVariables().putObject(getConnectionId(), connection);
		}
	}

	/** 
	 * The remote testStarted matches the normal testStarted function.
	 * */
	@Override
	public void testStarted(String arg0) {
		testStarted();
	}

	/**
	 * We will not add new ConfigElements.
	 * */
	@Override
	public void addConfigElement(ConfigElement arg0) {
		// TODO Auto-generated method stub
	}

	/** We do not expect modifications. */
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

	public String getJdbcName() {
		return getPropertyAsString(JDBC_NAME);
	}

	public void setJdbcName(String jdbcName) {
		setProperty(JDBC_NAME, jdbcName);
	}

	public String getClassName() {
		return getPropertyAsString(CLASS_NAME);
	}

	public void setClassName(String className) {
		setProperty(CLASS_NAME, className);
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

	public String getSid() {
		return getPropertyAsString(SID);
	}

	public void setSid(String sid) {
		setProperty(SID, sid);
	}

	public String getAutoCommit() {
		return getPropertyAsString(AUTO_COMMIT);
	}

	public void setAutoCommit(String autoCommit) {
		setProperty(AUTO_COMMIT, autoCommit);
	}

	public String getReadOnly() {
		return getPropertyAsString(READ_ONLY);
	}

	public void setReadOnly(String readOnly) {
		setProperty(READ_ONLY, readOnly);
	}

	/*public String getTransactionIsolation() {
		return getPropertyAsString(TRANSACTION_ISOLATION);
	}

	public void setTransactionIsolation(String transactionIsolation) {
		setProperty(TRANSACTION_ISOLATION, transactionIsolation);
	}*/

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
