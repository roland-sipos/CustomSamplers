package hypertable;

import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplerUtils;
import utils.CustomSamplersException;
import utils.QueryHandler;
import assignment.Assignment;
import assignment.AssignmentConfigElement;

public class HypertableSampler extends AbstractSampler implements TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = 5936080099656997612L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** This field indicates which HypertableConfigElement will be used for the sampling. */
	public final static String CONNECTION_ID = "HypertableSampler.connectionId";
	/** This field indicates, if the sampler need to close the fetched resource. */
	public final static String CLOSE_CONNECTION = "HypertableSampler.closeConnection";
	/** This field indicates which Assignment ConfigElement will be used for the sampling. */
	public final static String ASSIGNMENT_INFO = "HypertableSampler.assignmentInfo";
	/** This field indicates, if the sampling will use chunks of the payloads. */
	public final static String USE_CHUNKS = "HypertableSampler.useChunks";
	/** This field indicates, which I/O operation the sampling will do. */
	public final static String REQUEST_TYPE = "HypertableSampler.requestType";
	/** This field indicates, if the sampling will validate the operations. */
	public final static String VALIDATE_OPERATION = "HypertableSampler.validateOperation";

	public HypertableSampler() {
		trace("HypertableSampler() " + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		trace("sample() ThreadID: " + Thread.currentThread().getName());

		/** Fetch Assignment and QueryHandler instances. */
		QueryHandler queryHandler = null;
		Assignment assignment = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			/*if (getUseChunks().equals("bulk")) {
				queryHandler = new HypertableBulkQueryHandler(getConnectionId());
			} else if (getUseChunks().equals("bigtable")){
				queryHandler = new HypertableBigtableQueryHandler(getConnectionId());
			} else {*/
				queryHandler = new HypertableQueryHandler(getConnectionId());
			//}
		} catch (Exception e) {
			log.error("Failed to create HypertableSampler prerequisites for the " + 
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

		/** Hypertable Sampler may request manual close of resources. */
		if (Boolean.parseBoolean(getCloseConnection())) {
			try {
				queryHandler.closeResources();
			} catch (CustomSamplersException e) {
				log.error("Failed to close HypertableSampler connection resources for the " + 
						Thread.currentThread().getName() + " sampler. Details:" + e.toString());
				return CustomSamplerUtils.getExceptionSampleResult(e);
			}
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
		System.out.println("USE CHUNKS??? -> " + options.get("useChunks"));
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
	public String getCloseConnection() {
		return getPropertyAsString(CLOSE_CONNECTION);
	}
	public void setCloseConnection(String closeConnection) {
		setProperty(CLOSE_CONNECTION, closeConnection);
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
