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
	 * -- assignmentInputFile <br>
	 * -- assignmentOutputFile <br>
	 * binary <br>
	 * -- location <br>
	 * -- encoding <br>
	 * options <br>
	 * -- assignmentMode
	 * */
	public AssignmentConfigElementBeanInfo() {
		super(AssignmentConfigElement.class);

		createPropertyGroup("assignment", new String[] {"assignmentInfo",
				"assignmentInputFile", "assignmentOutputFile"});
		createPropertyGroup("binary", new String[] {"inputLocation", "encoding"});
		createPropertyGroup("options", new String[] {"assignmentMode"});

		PropertyDescriptor p = property("assignmentInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "assignInfo");
		p = property("assignmentInputFile");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "");
		p = property("assignmentOutputFile");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "");

		p = property("inputLocation");
		p.setValue(NOT_UNDEFINED,  Boolean.TRUE);
		p.setValue(DEFAULT, "/testdata/");
		p = property("encoding");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"binary", "base64"});
		p.setValue(NOT_OTHER, new String[] {"binary", "base64"});
		p.setValue(DEFAULT, "binary");

		p = property("assignmentMode");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(TAGS, new String[] {"random", "assigned", "mixed", "sequence"});
		p.setValue(DEFAULT, "random");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}


}
