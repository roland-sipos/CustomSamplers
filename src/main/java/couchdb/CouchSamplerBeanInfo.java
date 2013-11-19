package couchdb;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class CouchSamplerBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public CouchSamplerBeanInfo() {
		super(CouchSampler.class);

		createPropertyGroup("couch", new String[] {"database"});
		createPropertyGroup("sampler",
				new String[] {"assignmentInfo", "useChunks", "attachmentMode"});
		createPropertyGroup("reading", new String[] {"doRead", "checkRead"});
		createPropertyGroup("writing", new String[] {"doWrite"});

		PropertyDescriptor p = property("database");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "test");

		p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("useChunks");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.FALSE);
		p = property("attachmentMode");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.FALSE);

		p = property("doRead");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.TRUE);
		p = property("checkRead");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.FALSE);

		p = property("doWrite");
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
