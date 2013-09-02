package mongodb;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryconfig.BinaryConfigElement;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;


public class MongoSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = -5711822313690043207L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "MongoSampler.database";
	public final static String COLLECTION = "MongoSampler.collection";
	public final static String BINARYINFO = "CassandraSampler.binaryInfo";
	public final static String GRIDFSMETHOD = "MongoSampler.gridFsMethod";
	public final static String DOREAD = "MongoSampler.doRead";
	public final static String USERANDOMACCESS = "MongoSampler.useRandomAccess";
	public final static String CHECKREAD = "MongoSampler.checkRead";
	public final static String DOWRITE = "MongoSampler.doWrite";
	public final static String ASSIGNED_WRITE = "MongoSampler.assignedWrite";
	
	
	public MongoSampler() {
		trace("MongoSampler()" + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		BinaryFileInfo binaryInfo = null;
		MongoQueryHandler queryHandler = null;
		try {
			binaryInfo = BinaryConfigElement.getBinaryFileInfo(getBinaryInfo());
			queryHandler = new MongoQueryHandler(getDatabase(), getCollection());
		} catch (Exception e) {
			log.error("Failed to create a MongoQueryHandler instance for the " + 
					  Thread.currentThread().getName() + " sampler. Details:" + e.toString());
		}

		// Get an initial SampleResult and start it.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
		res.sampleStart();
		
        if (Boolean.parseBoolean(getDoRead())) { // DO THE READ
        	CustomSamplerUtils.doReadWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getCheckRead()), Boolean.parseBoolean(getGridFsMethod()));
        } else if (Boolean.parseBoolean(getDoWrite())) { // DO THE WRITE
        	CustomSamplerUtils.doWriteWith(queryHandler, binaryInfo, res, 
					Boolean.parseBoolean(getAssignedWrite()), Boolean.parseBoolean(getGridFsMethod()));
        }
        
        return res;
	}

	
	/*private void readFromMongo(MongoQueryHandler queryHandler, SampleResult res) {
		HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
		String originalID = hashes.get("originalID");
		String chunkID = hashes.get("chunkID");
		
		try {
			byte[] result = null;
			if (Boolean.parseBoolean(getGridFsMethod()))
				result = queryHandler.readFileFromMongo(chunkID + ".bin");
			else
				result = queryHandler.readBinaryFromMongo(
						originalID, chunkID, hashes.get("original") + "__" + hashes.get("chunk"));

			if (result == null)
				CustomSamplerUtils.finalizeResponse(res, false, "500", "The result is empty!");
			
			if (Boolean.parseBoolean(getCheckRead())) {
				String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
				byte[] fileContent = binaryInfo.read(filePath);
				if (!Arrays.equals(result, fileContent))
					CustomSamplerUtils.finalizeResponse(res, false, "500", "Read value is not correct!");
			}
			
			CustomSamplerUtils.finalizeResponse(res, true, "200",
					"Value read for:" + " B:" + originalID + " C:" + chunkID + " Success!");
			
		} catch (CustomSamplersException ex) {
			log.error("MongoSampler read attempt failed: " + ex.toString());
			CustomSamplerUtils.finalizeResponse(res, false, "500", ex.toString());
		} finally {
			res.sampleEnd();
		}
	}*/
	
	
	/*private void writeToMongo(MongoQueryHandler queryHandler, SampleResult res) {
		if (Boolean.parseBoolean(getAssignedWrite())) {
    		String chunkID = "chunk-" + CustomSamplerUtils.getThreadID(Thread.currentThread().getName()) + ".bin";
    		String pathToChunk = binaryInfo.getBinaryFilePathList().get("BIGrbinary-0.bin.chunks").get(chunkID);
    		byte[] chunkContent = binaryInfo.read(pathToChunk);
    		HashMap<String, String> hashes = binaryInfo.getHashesForIDs("BIGrbinary-0.bin.chunks", chunkID);
    		try {
    			if (Boolean.parseBoolean(getGridFsMethod()))
    				queryHandler.writeFileToMongo("BIGrbinary-0.bin.chunks", chunkID, 
    						hashes.get("original") + "__" + hashes.get("chunk"), chunkContent);
    			else
    				queryHandler.writeBinaryToMongo("BIGrbinary-0.bin.chunks", chunkID, 
    						hashes.get("original") + "__" + hashes.get("chunk"), chunkContent);
    			
    			CustomSamplerUtils.finalizeResponse(res, true, "200",
    					"Value read for:" + " B: BIGrbinary-0.bin.chunks" + " C:" + chunkID + " Success!");
    			
			} catch (CustomSamplersException ex) {
				log.error("MongoSampler write attempt failed: " + ex.toString());
				CustomSamplerUtils.finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
    		
    	}*/ /*else {
    		HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
    		String originalID = hashes.get("originalID");
    		String chunkID = hashes.get("chunkID");
    		String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
    		byte[] fileContent = binaryInfo.read(filePath);
    		try {
				queryHandler.writeBinaryToMongo(originalID, chunkID, 
												hashes.get("original") + "__" + hashes.get("chunk"),
												fileContent);
				
				CustomSamplerUtils.finalizeResponse(res, true, "200", 
						"Value written for:" + " B:" + originalID + " C:" + chunkID + " With GFS?:" + useGridFS + " Success!");

			} catch (CustomSamplersException ex) {
				log.error("MongoSampler write attempt failed: " + ex.toString());
				CustomSamplerUtils.finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
    	}*/
	//}
	
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
	public String getBinaryInfo() {
		return getPropertyAsString(BINARYINFO);
	}
	public void setBinaryInfo(String binaryInfo) {
		setProperty(BINARYINFO, binaryInfo);
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
