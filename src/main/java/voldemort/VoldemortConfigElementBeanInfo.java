package voldemort;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class VoldemortConfigElementBeanInfo extends BeanInfoSupport {

	private final static Logger log = LoggingManager.getLoggerForClass();

	public VoldemortConfigElementBeanInfo() {
		super(VoldemortConfigElement.class);

		createPropertyGroup("voldemort", new String[] { "host", "port",
				"database", "store", "username", "password" });

		createPropertyGroup("options", new String[] { "maxThreads",
				"maxQueuedRequests", "maxConnectionsPerNode",
				"maxTotalConnections", "socketTimeout", "connectionTimeout",
				"routingTimeout", "socketBufferSize" });

		PropertyDescriptor p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "tcp://testdb-pc.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "6666");
		p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "test");
		p = property("store");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testStore");
		p = property("username");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testUser");
		p = property("password");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testPass");

		p = property("maxThreads");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "10");
		p = property("maxQueuedRequests");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "500");
		p = property("maxConnectionsPerNode");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "10");
		p = property("maxTotalConnections");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "5000");
		p = property("socketTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "50000");
		p = property("connectionTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "15000");
		p = property("routingTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "15000");
		p = property("socketBufferSize");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "15728640");

		if (log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
