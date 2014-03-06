package customjdbc;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the CustomJDBCConnectionSampler class.
 * */
public class CustomJDBCConnectionSamplerBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the CustomJDBCConnnectionSampler class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * connection <br>
	 * -- connectionId <br>
	 * -- jdbcName <br>
	 * -- className <br>
	 * -- host <br>
	 * -- port <br>
	 * -- database <br>
	 * -- sid <br>
	 * -- autoCommit <br>
	 * -- username <br>
	 * -- password <br>
	 * */
	public CustomJDBCConnectionSamplerBeanInfo() {
		super(CustomJDBCConnectionSampler.class);

		createPropertyGroup("connection", new String[] {
				"connectionId", "jdbcName", "className", 
				"host", "port", "database", "sid", "autoCommit", 
				"username", "password"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "postgreConn");
		p = property("jdbcName");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "jdbc:postgresql");
		p = property("className");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "org.postgresql.Driver");
		p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "cloud-postgre.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "5432");
		p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testdb");
		p = property("sid");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "");
		p = property("autoCommit");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.TRUE);
		p = property("username");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "postgres");
		p = property("password");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testPass");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
