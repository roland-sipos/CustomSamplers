package mongodb;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;
import utils.CustomSamplersException;

public class MongoSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -5711822313690043207L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "MongoSampler.database";
	public final static String COLLECTION = "MongoSampler.collection";
	public final static String INPUTLOCATION = "MongoSampler.inputlocation";
	public final static String GRIDFSMETHOD = "MongoSampler.gridFsMethod";
	public final static String DOREAD = "MongoSampler.doRead";
	public final static String USERANDOMACCESS = "MongoSampler.useRandomAccess";
	public final static String CHECKREAD = "MongoSampler.checkRead";
	public final static String DOWRITE = "MongoSampler.doWrite";
	public final static String ASSIGNED_WRITE = "MongoSampler.assignedWrite";
	
	private static BinaryFileInfo binaryInfo;
	
	public MongoSampler() {
		// fields = null;
		binaryInfo = null;
		trace("MongoSampler()" + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
		String threadName = Thread.currentThread().getName();
		
		MongoQueryHandler queryHandler = null;
		try {
			queryHandler = new MongoQueryHandler(getDatabase(), getCollection());
		} catch (Exception e) {
			log.error("Failed to create a MongoQueryHandler instance for the " + threadName + " sampler. Details:" + e.toString());
		}
		
		Boolean doRead = Boolean.parseBoolean(getDoRead());
		Boolean doWrite = Boolean.parseBoolean(getDoWrite());
		Boolean useGridFS = Boolean.parseBoolean(getGridFsMethod());
		Boolean isAssignedW = Boolean.parseBoolean(getAssignedWrite());
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
        res.sampleStart();
		
        if (doRead) { // DO THE READ
        	HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
    		String originalID = hashes.get("originalID");
    		String chunkID = hashes.get("chunkID");
    		//String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
    		//byte[] fileContent = binaryInfo.read(filePath);
    		try {
    			byte[] result = null;
    			if (useGridFS) {
    				result = queryHandler.readFileFromMongo(chunkID + ".bin");
    			} else {
    				result = queryHandler.readBinaryFromMongo(
    						originalID, chunkID, hashes.get("original") + "__" + hashes.get("chunk"));
    			}
    			if (result == null) {
    				log.error("MongoSampler random read failed!");
    				res.setResponseCode("500");
        			res.setSuccessful(false);
        			res.setResponseMessage("YAY THE VALUE IS EMPTY!!!!!!!!!!");
        			res.setResponseData("YAY THE VALUE IS EMPTY!!!!!!".getBytes());
    			}
    			/*if (!Arrays.equals(result, fileContent)) {
    				log.error("MongoSampler random read failed!");
    				res.setResponseCode("500");
        			res.setSuccessful(false);
        			res.setResponseMessage("YAY THE VALUE IS DIFFFEEEEEEEERRRRRRRRSSSS!!!!!!!!!!");
        			res.setResponseData("YAY THE VALUE IS DIFFFEEEEEEEEEEEEERSSSSSS!!!!!!".getBytes());
    			}*/
    			res.latencyEnd();			
    			String responseStr = "Value read for:" + " B:" + originalID + " C:" + chunkID + " Success!";
    			res.setResponseData(responseStr.getBytes());
			} catch (CustomSamplersException ex) {
				log.error("MongoSampler read attempt failed: " + ex.toString());
				res.setResponseCode("500");
    			res.setSuccessful(false);
    			res.setResponseMessage(ex.toString());
    			res.setResponseData(ex.getMessage().getBytes());
			} finally {
				res.sampleEnd();
			}
        } else if (doWrite) { // DO THE WRITE
        	if (isAssignedW) {
        		String chunkID = "chunk-" + threadID + ".bin";
        		String pathToChunk = binaryInfo.getBinaryFilePathList().get("BIGrbinary-0.bin.chunks").get(chunkID);
        		byte[] chunkContent = binaryInfo.read(pathToChunk);
        		HashMap<String, String> hashes = binaryInfo.getHashesForIDs("BIGrbinary-0.bin.chunks", chunkID);
        		try {
        			if (useGridFS) {
        				queryHandler.writeFileToMongo("BIGrbinary-0.bin.chunks", chunkID, 
        						hashes.get("original") + "__" + hashes.get("chunk"), chunkContent);
        			} else {
        				queryHandler.writeBinaryToMongo("BIGrbinary-0.bin.chunks", chunkID, 
        						hashes.get("original") + "__" + hashes.get("chunk"), chunkContent);
        			}
					res.latencyEnd();			
					String responseStr = "Value written for:" + " B: BIGrbinary-0.bin.chunks" + " C:" + chunkID + " With GFS?:" + useGridFS + " Success!";
	    			res.setResponseData(responseStr.getBytes());
				} catch (CustomSamplersException ex) {
					log.error("MongoSampler write attempt failed: " + ex.toString());
					res.setResponseCode("500");
        			res.setSuccessful(false);
        			res.setResponseMessage(ex.toString());
        			res.setResponseData(ex.getMessage().getBytes());
				} finally {
					res.sampleEnd();
				}
        	} else {
        		HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
        		String originalID = hashes.get("originalID");
        		String chunkID = hashes.get("chunkID");
        		String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
        		byte[] fileContent = binaryInfo.read(filePath);
        		try {
					queryHandler.writeBinaryToMongo(originalID, chunkID, 
													hashes.get("original") + "__" + hashes.get("chunk"),
													fileContent);
					res.latencyEnd();			
					String responseStr = "Value written for:" + " B:" + originalID + " C:" + chunkID + " With GFS?:" + useGridFS + " Success!";
	    			res.setResponseData(responseStr.getBytes());
				} catch (CustomSamplersException ex) {
					log.error("MongoSampler write attempt failed: " + ex.toString());
					res.setResponseCode("500");
        			res.setSuccessful(false);
        			res.setResponseMessage(ex.toString());
        			res.setResponseData(ex.getMessage().getBytes());
				} finally {
					res.sampleEnd();
				}
        	}
        }
        
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
	public String getCollection() {
		return getPropertyAsString(COLLECTION);
	}
	public void setCollection(String collection) {
		setProperty(COLLECTION, collection);
	}
	public String getInputLocation() {
		return getPropertyAsString(INPUTLOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUTLOCATION, inputLocation);
	}
	public String getGridFsMethod() {
		return getPropertyAsString(GRIDFSMETHOD);
	}
	public void setGridFsMethod(String gridFsMethod) {
		setProperty(GRIDFSMETHOD, gridFsMethod);
	}
	public String getUseRandomAccess() {
		return getPropertyAsString(USERANDOMACCESS);
	}
	public void setUseRandomAccess(String useRandomAccess) {
		setProperty(USERANDOMACCESS, useRandomAccess);
	}
	public String getChechRead() {
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
