package cassandra;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class CassandraConfigElementBeanInfo extends BeanInfoSupport {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public CassandraConfigElementBeanInfo() {
		super(CassandraConfigElement.class);

		createPropertyGroup("cassandra", new String[] {
				"connectionId", "host", "port", "cluster" });
		createPropertyGroup("hostconfig", new String[] {
				"thriftSocketTimeout",
				"retryDownedHostDelaySec",
				"retryDownedHostQueueSize",
				"maxWaitTimeIfExhausted",
				"retryDownedHosts",
				"loadBalancingPolicy",
				"setAutoDiscoverHosts",
				"setHostTimeoutCounter"});

		PropertyDescriptor p = property("connectionId");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "cassConn");
		p = property("host");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "cloudssd-cassandra.cern.ch");
		p = property("port");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "9160");
		p = property("cluster");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "TestCondCass");

		p = property("thriftSocketTimeout");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "10000");
		p = property("retryDownedHostDelaySec");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "30");
		p = property("retryDownedHostQueueSize");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "128");
		p = property("maxWaitTimeIfExhausted");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "60000");
		p = property("retryDownedHosts");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.TRUE);
		p = property("loadBalancingPolicy");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "leastActive");
		p = property("setAutoDiscoverHosts");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, Boolean.TRUE);
		p = property("setHostTimeoutCounter");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "20");

		if(log.isDebugEnabled()) {
			for (PropertyDescriptor pd : getPropertyDescriptors()) {
				log.debug(pd.getName());
				log.debug(pd.getDisplayName());
			}
		}

	}

}
