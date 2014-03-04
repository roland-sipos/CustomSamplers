package customjdbc;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the CustomJDBCConfigElement class.
 * */
public class CustomJDBCConfigElementBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the CustomJDBCConfigElement class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * connection <br>
	 * -- connectionId <br>
	 * -- jdbcname <br>
	 * -- classname <br>
	 * -- host <br>
	 * -- port <br>
	 * -- database <br>
	 * -- sid <br>
	 * -- autocommit <br>
	 * -- username <br>
	 * -- password <br>
	 * */
	public CustomJDBCConfigElementBeanInfo() {
		super(CustomJDBCConfigElement.class);

		createPropertyGroup("connection", new String[] {
				"connectionId", "jdbcname", "classname", 
				"host", "port", "database", "sid", "autocommit", 
				"username", "password"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "postgreConn");
		p = property("jdbcname");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "jdbc:postgresql");
		p = property("classname");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "org.postgresql.Driver");
		p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "137.138.229.253");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "5432");
		p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testdb");
		p = property("sid");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "");
		p = property("autocommit");
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
