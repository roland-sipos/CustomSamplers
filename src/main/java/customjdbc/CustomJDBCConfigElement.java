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


public class CustomJDBCConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 6256939089220907405L;

	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String JDBCNAME = "CustomJDBCConfigElement.jdbcname";
	public final static String CLASSNAME = "CustomJDBCConfigElement.classname";
	public final static String HOST = "CustomJDBCConfigElement.host";
	public final static String PORT = "CustomJDBCConfigElement.port";
	public final static String DATABASE = "CustomJDBCConfigElement.database";
	public final static String SID = "CustomJDBCConfigElement.sid";
	public final static String AUTOCOMMIT = "CustomJDBCConfigElement.autocommit";
	public final static String USERNAME = "CustomJDBCConfigElement.username";
	public final static String PASSWORD = "CustomJDBCConfigElement.password";

	@Override
	public void testEnded() {
		Object connectionObject =
				JMeterContextService.getContext().getVariables().getObject(getDatabase());
		if (connectionObject == null) {
			log.error("JDBC Connection object is null!");
		}
		else {
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
		getThreadContext().getVariables().putObject(getDatabase(), null);
	}
	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static Connection createJDBCConnection(String jdbcName, String host,
			String port, String sid, String database, String username, String password) {
		Connection connection = null;
		String connectionStr = jdbcName + "://" + host + ":" + port + "/" + database;
		if (!sid.isEmpty()) {
			connectionStr = jdbcName + ":@" + host + ":" + port + ":" + database;
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

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}

		try {
			Class.forName(getClassname());
		} catch (ClassNotFoundException e) {
			log.error("JDBC Driver class not found for: " + getClassname() + " Exception:" + e);
		}

		if (getThreadContext().getVariables().getObject(getDatabase()) != null) {
			log.warn(getDatabase() + " has already initialized!");
		} else {
			if (log.isDebugEnabled()) {
				log.debug(getDatabase() + " is being initialized ...");
			}

			Connection connection = createJDBCConnection(getJdbcname(), getHost(),
					getPort(), getSid(), getDatabase(), getUsername(), getPassword());

			try {
				connection.setAutoCommit(Boolean.parseBoolean(getAutocommit()));
			} catch (SQLException e) {
				log.error("Failed to change autoCommit to false: " + e.toString());
			}

			getThreadContext().getVariables().putObject(getDatabase(), connection);
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


	public String getJdbcname() {
		return getPropertyAsString(JDBCNAME);
	}

	public void setJdbcname(String jdbcname) {
		setProperty(JDBCNAME, jdbcname);
	}

	public String getClassname() {
		return getPropertyAsString(CLASSNAME);
	}

	public void setClassname(String classname) {
		setProperty(CLASSNAME, classname);
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

	public String getAutocommit() {
		return getPropertyAsString(AUTOCOMMIT);
	}

	public void setAutocommit(String autocommit) {
		setProperty(AUTOCOMMIT, autocommit);
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
