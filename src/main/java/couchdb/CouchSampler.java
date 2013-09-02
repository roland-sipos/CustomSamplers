package couchdb;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryconfig.BinaryConfigElement;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

public class CouchSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -6487975803592558073L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "CouchSampler.database";
	public final static String BINARYINFO = "CouchSampler.binaryInfo";
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

		// Get BinaryInfo and QueryHandler instances.
		BinaryFileInfo binaryInfo = null;
		CouchQueryHandler queryHandler = null;
		try {
			binaryInfo = BinaryConfigElement.getBinaryFileInfo(getBinaryInfo());
			queryHandler = new CouchQueryHandler(getDatabase(), "");
		} catch (Exception e) {
			log.error("Failed to create a CouchQueryHandler instance for the " + 
					  Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}
		
		// Get an initial SampleResult.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
	
		if(Boolean.parseBoolean(getDoRead())) // DO THE READ
			CustomSamplerUtils.doReadWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getCheckRead()), Boolean.parseBoolean(getAttachmentMode()));
		else if (Boolean.parseBoolean(getDoWrite())) // DO THE WRITE
			CustomSamplerUtils.doWriteWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getAssignedWrite()), Boolean.parseBoolean(getAttachmentMode()));
		
		return res;
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
