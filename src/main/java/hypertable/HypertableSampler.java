package hypertable;

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

public class HypertableSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 5936080099656997612L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CLUSTER = "HypertableSampler.cluster";
	public final static String ASSIGNMENTINFO = "HypertableSampler.assignmentInfo";
	public final static String USECHUNKS = "HypertableSampler.useChunks";
	public final static String DOREAD = "HypertableSampler.doRead";
	public final static String USERANDOMACCESS = "HypertableSampler.useRandomAccess";
	public final static String CHECKREAD = "HypertableSampler.checkRead";
	public final static String DOWRITE = "HypertableSampler.doWrite";
	public final static String ASSIGNED_WRITE = "HypertableSampler.assignedWrite";
	
	public HypertableSampler() {
		trace("HypertableSampler() " + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);

		// Get Assignment and QueryHandler instances.
		Assignment assignment = null;
		QueryHandler queryHandler = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			if (getUseChunks().equals("bulk")) {
				queryHandler = new HypertableBulkQueryHandler(getCluster());
			} else if (getUseChunks().equals("bigtable")){
				queryHandler = new HypertableBigtableQueryHandler(getCluster());
			} else {
				queryHandler = new HypertableQueryHandler(getCluster());
			}
		} catch (Exception e) {
			log.error("Failed to create HypertableSampler prerequisites for the " + 
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
		String cProp = getUseChunks();
		options.put("useChunks", cProp.equals(String.valueOf(Boolean.TRUE)) || cProp.equals("bulk"));
		options.put("isCheckRead", Boolean.parseBoolean(getCheckRead()));
		return options;
	}

	private void trace(String s) {
		if(log.isDebugEnabled())
			log.debug(Thread.currentThread().getName() + " (" + getTitle() + " " + s + " " + this.toString());
	}

	public String getTitle() {
		return this.getName();
	}
	public String getCluster() {
		return getPropertyAsString(CLUSTER);
	}
	public void setCluster(String cluster) {
		setProperty(CLUSTER, cluster);
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
