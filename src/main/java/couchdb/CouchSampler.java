package couchdb;

import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryconfig.BinaryConfigElement;
import binaryconfig.BinaryFileInfo;

import utils.CustomSamplerUtils;
import utils.QueryHandler;

public class CouchSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -6487975803592558073L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "CouchSampler.database";
	public final static String BINARYINFO = "CouchSampler.binaryInfo";
	public final static String USECHUNKS = "CouchSampler.useChunks";
	public final static String ATTACHMENT_MODE = "CouchSampler.attachmentMode";
	public final static String DOREAD = "CouchSampler.doRead";
	public final static String USERANDOMACCESS = "CouchSampler.useRandomAccess";
	public final static String CHECKREAD = "CouchSampler.checkRead";
	public final static String DOWRITE = "CouchSampler.doWrite";
	public final static String ASSIGNED_WRITE = "CouchSampler.assignedWrite";

	
	public CouchSampler() {
		trace("CouchSampler()" + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);

		// Get an initial SampleResult and parse options.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
		HashMap<String, Boolean> options = prepareOptions();

		// Get BinaryInfo and QueryHandler instances.
		BinaryFileInfo binaryInfo = null;
		QueryHandler queryHandler = null;
		try {
			binaryInfo = BinaryConfigElement.getBinaryFileInfo(getBinaryInfo());
			// TODO: IF KRYO METHOD, KRYO QueryHandler!!!!!
			if (Boolean.parseBoolean(getAttachmentMode())) {
				queryHandler = new CouchAttachmentQueryHandler(getDatabase());
			} else {
				queryHandler = new CouchQueryHandler(getDatabase());
			}
		} catch (Exception e) {
			log.error("Failed to create CouchSampler prerequisites for the " + 
					Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}

		if (options.get("doRead")) { // DO THE READ
			CustomSamplerUtils.readWith(queryHandler, binaryInfo, res, options);
		} else if (options.get("doWrite")) { // DO THE WRITE
			CustomSamplerUtils.writeWith(queryHandler, binaryInfo, res, options);
		}

		return res;
	}

	private HashMap<String, Boolean> prepareOptions() {
		HashMap<String, Boolean> options = new HashMap<String, Boolean>();
		options.put("doRead", Boolean.parseBoolean(getDoRead()));
		options.put("doWrite", Boolean.parseBoolean(getDoWrite()));
		options.put("useChunks", Boolean.parseBoolean(getUseChunks()));
		options.put("isRandom", Boolean.parseBoolean(getUseRandomAccess()));
		options.put("isCheckRead", Boolean.parseBoolean(getCheckRead()));
		options.put("isSpecial", Boolean.parseBoolean(getAttachmentMode()));
		options.put("isAssigned", Boolean.parseBoolean(getAssignedWrite()));
		return options;
	}

	private void trace(String s) {
		if(log.isDebugEnabled())
			log.debug(Thread.currentThread().getName() + " (" + getTitle() + " " + s + " " + this.toString());
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
	public String getUseChunks() {
		return getPropertyAsString(USECHUNKS);
	}
	public void setUseChunks(String useChunks) {
		setProperty(USECHUNKS, useChunks);
	}
	public String getAttachmentMode() {
		return getPropertyAsString(ATTACHMENT_MODE);
	}
	public void setAttachmentMode(String attachmentMode) {
		setProperty(ATTACHMENT_MODE, attachmentMode);
	}
	public String getUseRandomAccess() {
		return getPropertyAsString(USERANDOMACCESS);
	}
	public void setUseRandomAccess(String useRandomAccess) {
		setProperty(USERANDOMACCESS, useRandomAccess);
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
	public String getAssignedWrite() {
		return getPropertyAsString(ASSIGNED_WRITE);
	}
	public void setAssignedWrite(String assignedWrite) {
		setProperty(ASSIGNED_WRITE, assignedWrite);
	}

}
