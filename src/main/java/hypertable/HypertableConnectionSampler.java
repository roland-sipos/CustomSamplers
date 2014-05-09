package hypertable;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.threads.JMeterVariables;
import org.hypertable.thrift.ThriftClient;

import utils.CustomSamplerUtils;
import utils.CustomSamplersException;


public class HypertableConnectionSampler extends AbstractSampler implements TestBean {

	/** Generated UID */
	private static final long serialVersionUID = 6535326379998980656L;
	public final static String CONNECTION_ID = "HypertableConnectionSampler.connectionId";
	public final static String NAMESPACE = "HypertableConnectionSampler.namespace";
	public final static String HOST = "HypertableConnectionSampler.host";
	public final static String PORT = "HypertableConnectionSampler.port";
	public final static String TIMEOUT = "HypertableConnectionSampler.timeout";
	public final static String DO_OPEN = "HypertableConnectionSampler.doOpen";
	public final static String FRAME_SIZE = "HypertableConnectionSampler.frameSize";


	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getName());
		try {
			ThriftClient hyperTClient = null;
			Long hyperTNS = -1L;

			res.sampleStart();
			hyperTClient = ThriftClient.create(getHost(), Integer.valueOf(getPort()), 
					Integer.valueOf(getTimeout()), Boolean.parseBoolean(getDoOpen()),
					Integer.valueOf(getFrameSize()) * 1024 * 1024);
			res.samplePause();

			if (!hyperTClient.namespace_exists(getNamespace())) {
				throw new CustomSamplersException("Namespace " + getNamespace() + "doesn't exist!");
			} else {
				hyperTNS = hyperTClient.namespace_open(getNamespace());
			}

			JMeterVariables jMeterVars = getThreadContext().getVariables();
			// Put the Hypertable ThriftClient element into the ThreadContext.
			jMeterVars.putObject(getConnectionId(), hyperTClient);
			// Put the Hypertable Namespace elements into the ThreadContext.
			jMeterVars.putObject(getConnectionId().concat("-NS"), hyperTNS);
			System.out.println("-> passing: " + hyperTClient.toString());

		} catch (Exception e) {
			CustomSamplerUtils.finalizeResponse(res, false, "999",
					"Exception occured in this sample: " + e.toString());
		} finally {
			CustomSamplerUtils.finalizeResponse(res, true, "200", "ThriftClient Object is placed successfully, "
					+ "with ID: " + getConnectionId());
			res.sampleEnd();
		}
		return res;
	}


	public String getConnectionId() {
		return getPropertyAsString(CONNECTION_ID);
	}
	public void setConnectionId(String connectionId) {
		setProperty(CONNECTION_ID, connectionId);
	}

	public String getNamespace() {
		return getPropertyAsString(NAMESPACE);
	}
	public void setNamespace(String namespace) {
		setProperty(NAMESPACE, namespace);
	}

	public String getHost() {
		return getPropertyAsString(HOST);
	}
	public void setHost(String host) {
		setProperty(HOST, host);
	}

	public String getPort() {
		return getPropertyAsString(PORT);
	}
	public void setPort(String port) {
		setProperty(PORT, port);
	}

	public String getTimeout() {
		return getPropertyAsString(TIMEOUT);
	}
	public void setTimeout(String timeout) {
		setProperty(TIMEOUT, timeout);
	}

	public String getDoOpen() {
		return getPropertyAsString(DO_OPEN);
	}
	public void setDoOpen(String doOpen) {
		setProperty(DO_OPEN, doOpen);
	}

	public String getFrameSize() {
		return getPropertyAsString(FRAME_SIZE);
	}
	public void setFrameSize(String frameSize) {
		setProperty(FRAME_SIZE, frameSize);
	}

}
