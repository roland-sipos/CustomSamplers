package mongodb;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the MongoSampler class.
 * */
public class MongoSamplerBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the MongoSampler class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * mongo <br>
	 * -- connectionId <br>
	 * sampler <br>
	 * -- assignmentInfo <br>
	 * -- useChunks <br>
	 * -- gridFsMethod
	 * io <br>
	 * -- requestType <br>
	 * -- validateOperation <br>
	 * */
	public MongoSamplerBeanInfo() {
		super(MongoSampler.class);

		createPropertyGroup("mongo", new String[] {"connectionId"});
		createPropertyGroup("sampler", new String[] {"assignmentInfo", "useChunks", "gridFsMethod"});
		createPropertyGroup("io", new String[] {"requestType", "validateOperation"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "mongoConn");

		p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("useChunks");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "False");
		p = property("gridFsMethod");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "False");

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
