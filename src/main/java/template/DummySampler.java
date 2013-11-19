package template;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;

import utils.CustomSamplerUtils;


/**
 * This class is an example Sampler to present, how to add new elements
 * to the CustomSamplers project, and understand how the machinery works.
 * Every JMeter sampler needs to extend the AbstractSampler and
 * implement the TestBean classes.
 * */
public class DummySampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -337844041955076451L;
	/**
	 * With the resourceName field the user may request, which resource ID
	 * will be fetched from the JMeter context. (Of cource, if there is a 
	 * ConfigElement that already placed there.
	 * @see DummyResourceConfigElement
	 * */
	public final static String RESOURCENAME = "DummySampler.resourceName";

	/**
	 * In a JMeter testplan's ThreadGroup, every Thread calls this function.
	 * So basically one needs to create a SampleResult object, start a process
	 * or calculation that needs to be measured, set appropriate flags and messages
	 * in the SampleResult and finally return it.
	 * */
	@Override
	public SampleResult sample(Entry arg0) {

		SampleResult res = CustomSamplerUtils.getInitialSampleResult(this.getName());
		/**
		 * This Sampler uses some external resources that need to be provided through the
		 * JMeter's ThreadContext, and there is also a dedicated class that handles this resource.
		 * We would like to measure how long does it take for the Handler to manipulate the resource.
		 * */
		DummyResource resource = null;
		DummyResourceHandler handler = null;
		try {
			resource = DummyResourceConfigElement.getResource(getResourceName());
			handler = new DummyResourceHandler(resource);
		} catch (Exception e) {
			return CustomSamplerUtils.getExceptionSampleResult(e);
		}

		/**
		 * Let's sample what we want! Start the SampleResult's internal latency stopper,
		 * do the manipulation we would like to measure, and stop it finally. 
		 * */
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
