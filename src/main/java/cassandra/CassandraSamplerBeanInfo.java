package cassandra;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class CassandraSamplerBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public CassandraSamplerBeanInfo() {
		super(CassandraSampler.class);
		
		createPropertyGroup("cassandra", new String[] {"database", "keyspace", "columnFamily"});
		createPropertyGroup("sampler", new String[] {"binaryInfo"});

		createPropertyGroup("reading", new String[] {
				"doRead",
				"useRandomAccess",
				"checkRead"});
		
		createPropertyGroup("writing", new String[] {"doWrite", "assignedWrite"});
		
        PropertyDescriptor p = property("database");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "TestCondCass");
        p = property("keyspace");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "WriteTestKS");
        p = property("columnFamily");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "TestCF");
        
        
        p = property("binaryInfo");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "binInfo");
        
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
