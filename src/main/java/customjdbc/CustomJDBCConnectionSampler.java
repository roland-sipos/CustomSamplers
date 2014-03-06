package customjdbc;

import java.sql.Connection;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplerUtils;

/**
 * This class is the Sampler for JDBC Connection creation.
 * The member fields are the user options, set by the appropriate BeanInfo class.
 * */
public class CustomJDBCConnectionSampler extends AbstractSampler implements TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = 5383426919863326602L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CONNECTION_ID = "CustomJDBCConnectionSampler.connectionId";
	public final static String JDBC_NAME = "CustomJDBCConnectionSampler.jdbcName";
	public final static String CLASS_NAME = "CustomJDBCConnectionSampler.className";
	public final static String HOST = "CustomJDBCConnectionSampler.host";
	public final static String PORT = "CustomJDBCConnectionSampler.port";
	public final static String DATABASE = "CustomJDBCConnectionSampler.database";
	public final static String SID = "CustomJDBCConnectionSampler.sid";
	public final static String AUTO_COMMIT = "CustomJDBCConnectionSampler.autoCommit";
	public final static String USERNAME = "CustomJDBCConnectionSampler.username";
	public final static String PASSWORD = "CustomJDBCConnectionSampler.password";

	public CustomJDBCConnectionSampler() {
		trace("CustomJDBCConnectionSampler()" + this.toString());
	}

	/**
	 * The sample function is called by every thread that are defined in the current
	 * JMeter ThreadGroup. It creates a JDBC Connection and place it to the ThreadGroup's
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
			res.sampleStart();
			Connection connection = CustomJDBCConfigElement.createJDBCConnection(
					getJdbcName(), getHost(), getPort(), getSid(), getDatabase(),
					getUsername(), getPassword());
			res.samplePause();
			connection.setAutoCommit(Boolean.parseBoolean(getAutocommit()));
			getThreadContext().getVariables().putObject(getConnectionId(), connection);
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
		if(log.isDebugEnabled())
			log.debug(Thread.currentThread().getName() + " (" + getTitle() + " " + s + " " + this.toString());
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

	public String getAutocommit() {
		return getPropertyAsString(AUTO_COMMIT);
	}

	public void setAutoCommit(String autoCommit) {
		setProperty(AUTO_COMMIT, autoCommit);
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
