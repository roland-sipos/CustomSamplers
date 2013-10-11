package template;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;

public class DummySamplerBeanInfo extends BeanInfoSupport {

	public DummySamplerBeanInfo() {
		super(DummySampler.class);

		createPropertyGroup("dummy", new String[] {"resourceName"});

		PropertyDescriptor p = property("resourceName");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "dummyResource");

	}

}
