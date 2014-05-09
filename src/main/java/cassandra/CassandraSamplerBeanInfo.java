package cassandra;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class CassandraSamplerBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public CassandraSamplerBeanInfo() {
		super(CassandraSampler.class);

		createPropertyGroup("cassandra", new String[] {"connectionId"});
		createPropertyGroup("sampler", new String[] {"assignmentInfo", "useChunks"});
		createPropertyGroup("io", new String[] {"requestType", "validateOperation"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "TestCondCass");

		p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("useChunks");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"true", "false", "bulk"});
		p.setValue(DEFAULT, "false");

		p = property("requestType");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"read", "write"});
		p.setValue(NOT_OTHER, new String[] {"read", "write"});
		p.setValue(DEFAULT, "read");
		p = property("validateOperation");
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
