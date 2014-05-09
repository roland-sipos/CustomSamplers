package hypertable;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class HypertableConnectionSamplerBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public HypertableConnectionSamplerBeanInfo() {
		super(HypertableConnectionSampler.class);

		createPropertyGroup("hypertable", new String[] { "connectionId", "namespace" });
		createPropertyGroup("hypertableConfig", new String[] { "host", "port" });
		createPropertyGroup("thriftClient", new String[] { "timeout", "doOpen", "frameSize" });

		PropertyDescriptor p = property("connectionId");
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

		p = property("timeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "1600000");
		p = property("doOpen");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "True");
		p = property("frameSize");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "100");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}
	}

}
