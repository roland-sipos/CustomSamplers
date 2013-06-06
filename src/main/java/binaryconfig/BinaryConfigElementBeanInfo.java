package binaryconfig;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class BinaryConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public BinaryConfigElementBeanInfo() {
		super(BinaryConfigElement.class);
		
		createPropertyGroup("binary", new String[] { "inputLocation", "binaryInfo"});
			
		PropertyDescriptor p = property("inputLocation");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "/home/cb/INPUT");
		p = property("binaryInfo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "binInfo");
					
		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
	            log.debug(pd.getDisplayName());
	        }
	    }
		
	}

}
