package template;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;

import utils.CustomSamplerUtils;

public class DummySampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -337844041955076451L;
	public final static String RESOURCENAME = "DummySampler.resourceName";

	@Override
	public SampleResult sample(Entry arg0) {

		SampleResult res = CustomSamplerUtils.getInitialSampleResult(this.getName());
		DummyResource resource = null;
		DummyResourceHandler handler = null;
		try {
			resource = DummyResourceConfigElement.getResource(getResourceName());
			handler = new DummyResourceHandler(resource);
		} catch (Exception e) {
			return CustomSamplerUtils.getExceptionSampleResult(e);
		}

		// Let's measure what we want!
		res.sampleStart();
		handler.howLongDoesThisTake();
		res.sampleEnd();

		CustomSamplerUtils.finalizeResponse(res, Boolean.TRUE, "200", "WOOF WOOF!");
		return res;
	}

	public String getResourceName() {
		return getPropertyAsString(RESOURCENAME);
	}
	public void setResourceName(String resourceName) {
		setProperty(RESOURCENAME, resourceName);
	}

}
