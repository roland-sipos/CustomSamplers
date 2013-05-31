package riak;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

public class RiakSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 8935021647941079090L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String CLUSTER = "RiakSampler.database";
	public final static String BUCKET = "RiakSampler.collection";
	public final static String INPUTLOCATION = "RiakSampler.inputlocation";
	public final static String KRYO_METHOD = "RiakSampler.kryoMethod";
	public final static String DOREAD = "RiakSampler.doRead";
	public final static String USERANDOMACCESS = "RiakSampler.useRandomAccess";
	public final static String CHECKREAD = "RiakSampler.checkRead";
	public final static String DOWRITE = "RiakSampler.doWrite";
	public final static String ASSIGNED_WRITE = "RiakSampler.assignedWrite";
	
	public static BinaryFileInfo binaryInfo;
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
		RiakQueryHandler queryHandler = null;
		try {
			queryHandler = new RiakQueryHandler(getCluster(), getBucket());
		} catch (Exception e) {
			log.error("Failed to create a RiakQueryHandler instance for the " + 
					  Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}
		
		// Get an initial SampleResult and start it.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
	
		if(Boolean.parseBoolean(getDoRead())) // DO THE READ
			CustomSamplerUtils.doReadWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getCheckRead()), Boolean.parseBoolean(getKryoMethod()));
		else if (Boolean.parseBoolean(getDoWrite())) // DO THE WRITE
			CustomSamplerUtils.doWriteWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getAssignedWrite()), Boolean.parseBoolean(getKryoMethod()));
		
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
	public String getCluster() {
		return getPropertyAsString(CLUSTER);
	}
	public void setCluster(String cluster) {
		setProperty(CLUSTER, cluster);
	}
	public String getBucket() {
		return getPropertyAsString(BUCKET);
	}
	public void setBucket(String bucket) {
		setProperty(BUCKET, bucket);
	}
	public String getInputLocation() {
		return getPropertyAsString(INPUTLOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUTLOCATION, inputLocation);
	}
	public String getKryoMethod() {
		return getPropertyAsString(KRYO_METHOD);
	}
	public void setKryoMethod(String kryoMethod) {
		setProperty(KRYO_METHOD, kryoMethod);
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
