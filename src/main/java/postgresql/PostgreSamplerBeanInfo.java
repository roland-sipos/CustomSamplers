package postgresql;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the PostgreSampler class.
 * */
public class PostgreSamplerBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the PostgreSampler class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * postgre <br>
	 * -- connectionId <br>
	 * sampler <br>
	 * -- assignmentInfo <br>
	 * -- useChunks <br>
	 * -- lobApi
	 * -- closeConnection
	 * io <br>
	 * -- requestType <br>
	 * -- validateOperation <br>
	 * */
	public PostgreSamplerBeanInfo() {
		super(PostgreSampler.class);

		createPropertyGroup("mongo", new String[] {"connectionId"});
		createPropertyGroup("sampler", new String[] {"assignmentInfo", "useChunks",
				"feature", "closeConnection"});
		createPropertyGroup("io", new String[] {"requestType", "validateOperation"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "postgreConn");

		p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("useChunks");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"True", "False"});
		p.setValue(NOT_OTHER, new String[] {"True", "False"});
		p.setValue(DEFAULT, "False");
		p = property("feature");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"json", "normal", "lobapi"});
		p.setValue(NOT_OTHER, new String[] {"json", "normal", "lobapi"});
		p.setValue(DEFAULT, "normal");
		p = property("closeConnection");
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
