package voldemort;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo; 
import utils.CustomSamplerUtils;
import binaryconfig.BinaryConfigElement;

public class VoldemortSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 2016246488713987591L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "VoldemortSampler.database";
	public final static String STORE = "VoldemortSampler.store";
	public final static String BINARYINFO = "VoldemortSampler.binaryInfo";
	public final static String DOREAD = "VoldemortSampler.doRead";
	public final static String USERANDOMACCESS = "VoldemortSampler.useRandomAccess";
	public final static String CHECKREAD = "VoldemortSampler.checkRead";
	public final static String DOWRITE = "VoldemortSampler.doWrite";
	public final static String ASSIGNED_WRITE = "VoldemortSampler.assignedWrite";
	
	public VoldemortSampler() {
		trace("VoldemortSampler()" + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		BinaryFileInfo binaryInfo = null;
		VoldemortQueryHandler queryHandler = null;
		try {
			binaryInfo = BinaryConfigElement.getBinaryFileInfo(getBinaryInfo());
			queryHandler = new VoldemortQueryHandler(getDatabase(), getStore());
		} catch (Exception e) {
			log.error("Failed to create a VoldemortQueryHandler instance for the " + 
					  Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}
		
		// Get an initial SampleResult and start it.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
	
		if(Boolean.parseBoolean(getDoRead())) // DO THE READ
			CustomSamplerUtils.doReadWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getCheckRead()), false);
		else if (Boolean.parseBoolean(getDoWrite())) // DO THE WRITE
			CustomSamplerUtils.doWriteWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getAssignedWrite()), false);
		
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
	public String getStore() {
		return getPropertyAsString(STORE);
	}
	public void setStore(String store) {
		setProperty(STORE, store);
	}
	public String getBinaryInfo() {
		return getPropertyAsString(BINARYINFO);
	}
	public void setBinaryInfo(String binaryInfo) {
		setProperty(BINARYINFO, binaryInfo);
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
