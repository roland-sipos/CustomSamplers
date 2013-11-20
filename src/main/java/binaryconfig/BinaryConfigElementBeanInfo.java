package binaryconfig;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 * This class is the BeanInfoSupport for the BinaryConfigElement class.
 * */
public class BinaryConfigElementBeanInfo extends BeanInfoSupport {

	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The constructor binds the given properties for the BinaryConfigElement class.
	 * <p>
	 * The property groups and properties are the following: <br>
	 * binary <br>
	 * -- inputLocation <br>
	 * -- binaryInfo <br>
	 * options <br>
	 * -- encoding
	 * */
	public BinaryConfigElementBeanInfo() {
		super(BinaryConfigElement.class);

		createPropertyGroup("binary", new String[] {"inputLocation", "binaryInfo"});
		createPropertyGroup("options", new String[] {"encoding"});

		PropertyDescriptor p = property("inputLocation");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "/home/cb/INPUT");

		p = property("binaryInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "binInfo");

		p = property("encoding");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "NO");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
