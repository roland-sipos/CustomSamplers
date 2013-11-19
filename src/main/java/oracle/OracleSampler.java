package oracle;

import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplerUtils;
import utils.CustomSamplersException;
import assignment.Assignment;
import assignment.AssignmentConfigElement;

public class OracleSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 2177540240166358248L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String DATABASE = "OracleSampler.database";
	public final static String BINARYINFO = "OracleSampler.binaryInfo";
	public final static String ASSIGNMENTINFO = "OracleSampler.assignmentInfo";
	public final static String USECHUNKS = "OracleSampler.useChunks";
	public final static String DOREAD = "OracleSampler.doRead";
	public final static String CHECKREAD = "OracleSampler.checkRead";
	public final static String DOWRITE = "OracleSampler.doWrite";

	public OracleSampler() {
		trace("OracleSampler()" + this.toString());
	}

	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);

		// Get Assignment and QueryHandler instances.
		Assignment assignment = null;
		OracleQueryHandler queryHandler = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			queryHandler = new OracleQueryHandler(getDatabase());
		} catch (CustomSamplersException e) {
			log.error("Failed to create a OracleSampler prerequisites for the " + 
					Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}

		// Get an initial SampleResult and parse options.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
		HashMap<String, Boolean> options = prepareOptions();
		
		if (options.get("doRead")) { // DO THE READ
			CustomSamplerUtils.readWith(queryHandler, assignment, res, options);
		} else if (options.get("doWrite")) { // DO THE WRITE
			CustomSamplerUtils.writeWith(queryHandler, assignment, res, options);
		}

		return res;
	}

	private HashMap<String, Boolean> prepareOptions() {
		HashMap<String, Boolean> options = new HashMap<String, Boolean>();
		options.put("doRead", Boolean.parseBoolean(getDoRead()));
		options.put("doWrite", Boolean.parseBoolean(getDoWrite()));
		options.put("useChunks", Boolean.parseBoolean(getUseChunks()));
		options.put("isCheckRead", Boolean.parseBoolean(getCheckRead()));
		options.put("isSpecial", false);
		return options;
	}
	
	private void trace(String s) {
		if(log.isDebugEnabled()) {
			log.debug(Thread.currentThread().getName() + " (" + getTitle() + " " + s + " " + this.toString());
		}
	}
	
	public String getTitle() {
		return this.getName();
	}
	public String getDatabase() {
		return getPropertyAsString(DATABASE);
	}
	public void setDatabase(String database) {
		setProperty(DATABASE, database);
	}
	public String getBinaryInfo() {
		return getPropertyAsString(BINARYINFO);
	}
	public void setBinaryInfo(String binaryInfo) {
		setProperty(BINARYINFO, binaryInfo);
	}
	public String getAssignmentInfo() {
		return getPropertyAsString(ASSIGNMENTINFO);
	}
	public void setAssignmentInfo(String assignmentInfo) {
		setProperty(ASSIGNMENTINFO, assignmentInfo);
	}
	public String getUseChunks() {
		return getPropertyAsString(USECHUNKS);
	}
	public void setUseChunks(String useChunks) {
		setProperty(USECHUNKS, useChunks);
	}
	public String getCheckRead() {
		return getPropertyAsString(CHECKREAD);
	}
	public void setCheckRead(String checkRead) {
		setProperty(CHECKREAD, checkRead);
	}
	public String getDoRead() {
		return getPropertyAsString(DOREAD);
	}
	public void setDoRead(String doRead) {
		setProperty(DOREAD, doRead);
	}
	public String getDoWrite() {
		return getPropertyAsString(DOWRITE);
	}
	public void setDoWrite(String doWrite) {
		setProperty(DOWRITE, doWrite);
	}
}
