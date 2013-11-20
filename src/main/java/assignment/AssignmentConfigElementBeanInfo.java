package assignment;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the AssignmentConfigElement class.
 * */
public class AssignmentConfigElementBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the AssignmentConfigElement class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * assignment <br>
	 * -- assignmentInfo <br>
	 * -- assignmentFile <br>
	 * options <br>
	 * -- binaryInfo
	 * -- assignmentMode
	 * */
	public AssignmentConfigElementBeanInfo() {
		super(AssignmentConfigElement.class);

		createPropertyGroup("assignment", new String[] {"assignmentInfo", "assignmentFile"});
		createPropertyGroup("options", new String[] {"binaryInfo", "assignmentMode"});

		PropertyDescriptor p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("assignmentFile");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "");

		p = property("binaryInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "binInfo");
		p = property("assignmentMode");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"random", "assigned", "mixed"});
		p.setValue(DEFAULT, "random");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}


}
