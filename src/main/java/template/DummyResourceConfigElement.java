package template;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;

import utils.CustomSamplersException;

public class DummyResourceConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -8862642361648122930L;
	private static final String RESOURCENAME = "DummyResourceConfigElement.resourceName";
	private static final String FOO = "DummyResourceConfigElement.foo";
	private static final String BAR = "DummyResourceConfigElement.bar";

	public static DummyResource getResource(String resourceName)
			throws CustomSamplersException {
		Object resource = JMeterContextService.getContext().getVariables().getObject(resourceName);
		if (resource == null) {
			throw new CustomSamplersException("DummyResource object is null!");
		}
		else {
			if (resource instanceof DummyResource) {
				return (DummyResource)resource;
			}
			else {
				throw new CustomSamplersException("Casting the object to DummyResource failed!");
			}
		}
	}

	@Override
	public void testEnded() {
		getThreadContext().getVariables().putObject(getResourceName(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	@Override
	public void testStarted() {
		if (getThreadContext().getVariables().getObject(getResourceName()) != null) {
			System.out.println(getResourceName() + " has already initialized!");
		} else {
			DummyResource resource = new DummyResource(getFoo(), getBar());
			getThreadContext().getVariables().putObject(getResourceName(), resource);
		}
	}

	@Override
	public void testStarted(String arg0) {
		testStarted();
	}

	@Override
	public void addConfigElement(ConfigElement arg0) {
	}

	@Override
	public boolean expectsModification() {
		return false;
	}

	public String getResourceName() {
		return getPropertyAsString(RESOURCENAME);
	}

	public void setResourceName(String resourceName) {
		setProperty(RESOURCENAME, resourceName);
	}

	public String getFoo() {
		return getPropertyAsString(FOO);
	}

	public void setFoo(String foo) {
		setProperty(FOO, foo);
	}

	public String getBar() {
		return getPropertyAsString(BAR);
	}

	public void setBar(String bar) {
		setProperty(BAR, bar);
	}
}

