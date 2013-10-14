package hbase;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class HBaseConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public HBaseConfigElementBeanInfo() {
		super(HBaseConfigElement.class);

		createPropertyGroup("hbase", new String[] { "clusterId"});

		createPropertyGroup("hbaseConfig", new String[] {
				"masterHost", "masterPort",
				"zookeeperQuorum", "zookeeperClientPort" });

		PropertyDescriptor p = property("clusterId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "TestHBase");

		p = property("masterHost");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "hb-master-test.cern.ch");
		p = property("masterPort");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "60000");

		p = property("zookeeperQuorum");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "hb-master-test.cern.ch");
		p = property("zookeeperClientPort");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "2181");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
