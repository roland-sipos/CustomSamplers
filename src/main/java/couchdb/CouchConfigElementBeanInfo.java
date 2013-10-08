package couchdb;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class CouchConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public CouchConfigElementBeanInfo() {
		super(CouchConfigElement.class);

		createPropertyGroup("couch", new String[] {
				"host", "port", "database", "username", "password"});
		createPropertyGroup("options", new String[] {
				"createIfNotExists"});

		PropertyDescriptor p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "http://testdb-pc.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "5984");
		p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "test");
		p = property("username");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testUser");
		p = property("password");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testPass");

		p = property("createIfNotExists");
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
