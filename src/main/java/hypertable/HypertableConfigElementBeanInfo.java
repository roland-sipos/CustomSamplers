package hypertable;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class HypertableConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public HypertableConfigElementBeanInfo() {
		super(HypertableConfigElement.class);

		createPropertyGroup("hypertable", new String[] { "clusterId", "namespace" });
		createPropertyGroup("hypertableConfig", new String[] { "host", "port" });

		PropertyDescriptor p = property("clusterId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "TestHypertable");
		p = property("namespace");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "testNamespace");

		p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "hypertable1.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "38080");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
