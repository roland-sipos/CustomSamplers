package riak;

import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import assignment.Assignment;
import assignment.AssignmentConfigElement;

import utils.CustomSamplerUtils;
import utils.QueryHandler;

public class RiakSampler extends AbstractSampler implements TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = 8935021647941079090L;
	/** Static logger from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** This field indicates which CustomJDBC ConfigElement will be used for the sampling. */
	public final static String CONNECTION_ID = "RiakSampler.connectionId";
	/** This field indicates which Assignment ConfigElement will be used for the sampling. */
	public final static String ASSIGNMENT_INFO = "RiakSampler.assignmentInfo";
	/** This field indicates, if the sampling will use chunks of the payloads. */
	public final static String USE_CHUNKS = "RiakSampler.useChunks";
	/** This field indicates, if the sampler will use the Kryo Method.*/
	public final static String KRYO_METHOD = "RiakSampler.kryoMethod";
	/** Option for using RIAK links (relation like method) or not.*/
	public final static String USE_LINKS = "RiakSampler.useLinks";
	/** This field indicates, which I/O operation the sampling will do. */
	public final static String REQUEST_TYPE = "RiakSampler.requestType";
	/** This field indicates, if the sampling will validate the operations. */
	public final static String VALIDATE_OPERATION = "RiakSampler.validateOperation";

	@Override
	public SampleResult sample(Entry arg0) {
		trace("sample() ThreadID: " + Thread.currentThread().getName());

		/** Fetch Assignment and QueryHandler instances. */
		QueryHandler queryHandler = null;
		Assignment assignment = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			// TODO: IF KRYO METHOD, KRYO QueryHandler!!!!!
			if (Boolean.parseBoolean(getKryoMethod())) {
				//queryHandler = new RiakKryoQueryHandler(getCluster());
			} else if (Boolean.parseBoolean(getUseLinks())) {
				queryHandler = new RiakLinkQueryHandler(getConnectionId());
			} else {
				queryHandler = new RiakQueryHandler(getConnectionId());
			}
		} catch (Exception e) {
			log.error("Failed to create RiakSampler prerequisites for the " + 
					Thread.currentThread().getName() + " sampler. Details:" + e.toString());
			return CustomSamplerUtils.getExceptionSampleResult(e);
		}

		/** Get an initial SampleResult and parse user options. */
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
		HashMap<String, Boolean> options = prepareOptions();

		/** Start the request, then return with the modified SampleResult. */
		if(getRequestType().equals("read")) {
			CustomSamplerUtils.readWith(queryHandler, assignment, res, options);
		} else if (getRequestType().equals("write")) {
			CustomSamplerUtils.writeWith(queryHandler, assignment, res, options);
		}
		return res;
	}

	/**
	 * This function parses the user options into a map.
	 * @return  HashMap<String, Boolean>  a map that contains the user options
	 * */
	private HashMap<String, Boolean> prepareOptions() {
		HashMap<String, Boolean> options = new HashMap<String, Boolean>();
		String cProp = getUseChunks();
		options.put("useChunks", cProp.equals(String.valueOf(Boolean.TRUE)));
		options.put("validateOperation", Boolean.parseBoolean(getValidateOperation()));
		return options;
	}

	/**
	 * Utility function for logging in the Sampler.
	 * @param  s  trace message
	 * */
	private void trace(String s) {
		if(log.isDebugEnabled())
			log.debug(Thread.currentThread().getName() + " (" + getTitle() + " " + s + " " + this.toString());
	}

	public String getTitle() {
		return this.getName();
	}
	public String getConnectionId() {
		return getPropertyAsString(CONNECTION_ID);
	}
	public void setConnectionId(String cluster) {
		setProperty(CONNECTION_ID, cluster);
	}
	public String getAssignmentInfo() {
		return getPropertyAsString(ASSIGNMENT_INFO);
	}
	public void setAssignmentInfo(String assignmentInfo) {
		setProperty(ASSIGNMENT_INFO, assignmentInfo);
	}
	public String getUseChunks() {
		return getPropertyAsString(USE_CHUNKS);
	}
	public void setUseChunks(String useChunks) {
		setProperty(USE_CHUNKS, useChunks);
	}
	public String getKryoMethod() {
		return getPropertyAsString(KRYO_METHOD);
	}
	public void setKryoMethod(String kryoMethod) {
		setProperty(KRYO_METHOD, kryoMethod);
	}
	public String getUseLinks() {
		return getPropertyAsString(USE_LINKS);
	}
	public void setUseLinks(String useLinks) {
		setProperty(USE_LINKS, useLinks);
	}
	public String getRequestType() {
		return getPropertyAsString(REQUEST_TYPE);
	}
	public void setRequestType(String requestType) {
		setProperty(REQUEST_TYPE, requestType);
	}
	public String getValidateOperation() {
		return getPropertyAsString(VALIDATE_OPERATION);
	}
	public void setValidateOperation(String validateOperation) {
		setProperty(VALIDATE_OPERATION, validateOperation);
	}

}
