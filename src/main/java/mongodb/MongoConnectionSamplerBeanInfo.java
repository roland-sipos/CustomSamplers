package mongodb;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the MongoConfigElement class.
 * */
public class MongoConnectionSamplerBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the MongoConnectionSampler class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * mongodb <br>
	 * -- connectionId <br>
	 * -- host <br>
	 * -- port <br>
	 * -- database <br>
	 * -- username <br>
	 * -- password <br>
	 * options <br>
	 * -- autoConnectRetry <br>
	 * -- connectionsPerHost <br>
	 * -- connectionTimeout <br>
	 * -- maxAutoConnectRetryTime <br>
	 * -- maxWaitTime <br>
	 * -- socketTimeout <br>
	 * -- socketKeepAlive <br>
	 * -- threadsAllowedToBlockMultiplier <br>
	 * */
	public MongoConnectionSamplerBeanInfo() {
		super(MongoConnectionSampler.class);

		createPropertyGroup("mongodb", new String[] {
				"connectionId", "host", "port", "database", "username", "password"});

		createPropertyGroup("options", new String[] {
				"autoConnectRetry",
				"connectionsPerHost",
				"connectTimeout",
				"maxAutoConnectRetryTime",
				"maxWaitTime",
				"socketTimeout",
				"socketKeepAlive",
				"threadsAllowedToBlockMultiplier"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "mongoConn");
		p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testdb-pc.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "27017");
		p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "test");
		p = property("username");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testUser");
		p = property("password");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testPass");

		p = property("autoConnectRetry");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "False");
		p = property("connectionsPerHost");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "10");
		p = property("connectTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "0");
		p = property("threadsAllowedToBlockMultiplier");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "5");
		p = property("maxAutoConnectRetryTime");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "0");
		p = property("maxWaitTime");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "120000");
		p = property("socketTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "0");
		p = property("socketKeepAlive");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "False");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
