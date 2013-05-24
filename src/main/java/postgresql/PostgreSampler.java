package postgresql;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

public class PostgreSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 5294863538969681929L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "PostgreSampler.database";
	public final static String INPUTLOCATION = "PostgreSampler.inputlocation";
	public final static String LARGEOBJECTMETHOD = "PostgreSampler.largeObjectMethod";
	public final static String DOREAD = "PostgreSampler.doRead";
	public final static String USERANDOMACCESS = "PostgreSampler.useRandomAccess";
	public final static String CHECKREAD = "PostgreSampler.checkRead";
	public final static String DOWRITE = "PostgreSampler.doWrite";
	public final static String ASSIGNED_WRITE = "PostgreSampler.assignedWrite";

	private static BinaryFileInfo binaryInfo;
	
	public PostgreSampler() {
		// fields = null;
		binaryInfo = null;
		trace("PostgreSampler()" + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
		PostgreQueryHandler queryHandler = null;
		try {
			queryHandler = new PostgreQueryHandler(getDatabase());
		} catch (Exception e) {
			log.error("Failed to create a MongoQueryHandler instance for the " + 
					  Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}

		// Get an initial SampleResult and start it.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
		res.sampleStart();
		CustomSamplerUtils.finalizeResponse(res, true, "200", "DUN..");
		return res;
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
	public String getInputLocation() {
		return getPropertyAsString(INPUTLOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUTLOCATION, inputLocation);
	}
	public String getLargeObjectMethod() {
		return getPropertyAsString(LARGEOBJECTMETHOD);
	}
	public void setLargeObjectMethod(String largeObjectMethod) {
		setProperty(LARGEOBJECTMETHOD, largeObjectMethod);
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
