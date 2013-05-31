package riak;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class RiakSamplerBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public RiakSamplerBeanInfo() {
		super(RiakSampler.class);
		
		createPropertyGroup("riak", new String[] {"cluster", "bucket"});
		createPropertyGroup("sampler", new String[] {"inputLocation", "kryoMethod"});

		createPropertyGroup("reading", new String[] {
				"doRead",
				"useRandomAccess",
				"checkRead"});
	
		createPropertyGroup("writing", new String[] {"doWrite", "assignedWrite"});
	
		PropertyDescriptor p = property("cluster");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "test");
		p = property("bucket");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "binaries");
		p = property("inputLocation");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "/home/cb/INPUT");
		p = property("kryoMethod");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.FALSE);
		
		p = property("doRead");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.TRUE);
		p = property("useRandomAccess");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "True");
		p = property("checkRead");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.FALSE);
    
		p = property("doWrite");
    	p.setValue(NOT_UNDEFINED, Boolean.TRUE);
    	p.setValue(DEFAULT, Boolean.FALSE);
    	p = property("assignedWrite");
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
