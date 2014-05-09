package hbase;

import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplerUtils;
import utils.QueryHandler;
import assignment.Assignment;
import assignment.AssignmentConfigElement;

public class HBaseSampler extends AbstractSampler implements TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = -3956828776033877346L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** This field indicates which HBaseConfigElement will be used for the sampling. */
	public final static String CONNECTION_ID = "HBaseSampler.connectionId";
	/** This field indicates which Assignment ConfigElement will be used for the sampling. */
	public final static String ASSIGNMENT_INFO = "HBaseSampler.assignmentInfo";
	/** This field indicates, if the sampling will use chunks of the payloads. */
	public final static String USE_CHUNKS = "HBaseSampler.useChunks";
	/** This field indicates, which I/O operation the sampling will do. */
	public final static String REQUEST_TYPE = "HBaseSampler.requestType";
	/** This field indicates, if the sampling will validate the operations. */
	public final static String VALIDATE_OPERATION = "HBaseSampler.validateOperation";

	public HBaseSampler() {
		trace("HBaseSampler() " + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		trace("sample() ThreadID: " + Thread.currentThread().getName());

		/** Fetch Assignment and QueryHandler instances. */
		QueryHandler queryHandler = null;
		Assignment assignment = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			if (getUseChunks().equals("bulk")) {
				queryHandler = new HBaseBulkQueryHandler(getConnectionId());
			} else {
				queryHandler = new HBaseQueryHandler(getConnectionId());
			}
		} catch (Exception e) {
			log.error("Failed to create HBaseSampler prerequisites for the " + 
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
	public void setConnectionId(String connectionId) {
		setProperty(CONNECTION_ID, connectionId);
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
