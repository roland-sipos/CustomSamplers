package template;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;

public class DummyResourceConfigElementBeanInfo extends BeanInfoSupport {

	public DummyResourceConfigElementBeanInfo() {
		super(DummyResourceConfigElement.class);

		createPropertyGroup("config", new String[] {"resourceName", "foo", "bar"});

		PropertyDescriptor p = property("resourceName");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "myDummyResource");
		
		p = property("foo");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "woof woof???");
		
		p = property("bar");
		p.setValue(NOT_UNDEFINED, Boolean.TRUE);
		p.setValue(DEFAULT, "foow foow!!!");
	}

}
