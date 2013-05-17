package mongodb;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class MongoConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public MongoConfigElementBeanInfo() {
		super(MongoConfigElement.class);
	
		createPropertyGroup("mongodb", new String[] {
	            "host", "port", "database", "username", "password"});

		createPropertyGroup("options", new String[] {
                "autoConnectRetry",
                "connectionsPerHost",
                "connectTimeout",
                "maxAutoConnectRetryTime",
                "maxWaitTime",
                "socketTimeout",
                "socketKeepAlive",
                "threadsAllowedToBlockMultiplier"});
		
		PropertyDescriptor p = property("host");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "137.138.229.253");
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
        p.setValue(DEFAULT, Boolean.FALSE);
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
        p.setValue(DEFAULT, Boolean.FALSE);
		
        if(log.isDebugEnabled()) {
            for (PropertyDescriptor pd : getPropertyDescriptors()) {
                log.debug(pd.getName());
                log.debug(pd.getDisplayName());
            }
        }
	
	}
	
}
