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

	private static final long serialVersionUID = 8935021647941079090L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CLUSTER = "RiakSampler.cluster";
	public final static String ASSIGNMENTINFO = "RiakSampler.assignmentInfo";
	public final static String USECHUNKS = "RiakSampler.useChunks";
	public final static String KRYO_METHOD = "RiakSampler.kryoMethod";
	public final static String USELINKS = "RiakSampler.useLinks";
	public final static String DOREAD = "RiakSampler.doRead";
	public final static String CHECKREAD = "RiakSampler.checkRead";
	public final static String DOWRITE = "RiakSampler.doWrite";

	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);

		// Get Assignment and QueryHandler instances.
		Assignment assignment = null;
		QueryHandler queryHandler = null;
		try {
			assignment = AssignmentConfigElement.getAssignments(getAssignmentInfo());
			// TODO: IF KRYO METHOD, KRYO QueryHandler!!!!!
			if (Boolean.parseBoolean(getKryoMethod())) {
				//queryHandler = new RiakKryoQueryHandler(getCluster());
			} else if (Boolean.parseBoolean(getUseLinks())) {
				queryHandler = new RiakLinkQueryHandler(getCluster());
			} else {
				queryHandler = new RiakQueryHandler(getCluster());
			}
		} catch (Exception e) {
			log.error("Failed to create RiakSampler prerequisites for the " + 
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
		options.put("isSpecial", Boolean.parseBoolean(getKryoMethod()));
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
	public String getKryoMethod() {
		return getPropertyAsString(KRYO_METHOD);
	}
	public void setKryoMethod(String kryoMethod) {
		setProperty(KRYO_METHOD, kryoMethod);
	}
	public String getUseLinks() {
		return getPropertyAsString(USELINKS);
	}
	public void setUseLinks(String useLinks) {
		setProperty(USELINKS, useLinks);
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
