/**
 * 
 */
package cassandra;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import exceptions.CustomSamplersException;
import exceptions.NotFoundInDBException;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

/**
 * @author cb
 *
 */
public class CassandraSampler extends AbstractSampler implements TestBean {

	public final static String DATABASE = "CassandraSampler.database";
	public final static String KEYSPACE = "CassandraSampler.keyspace";
	public final static String INPUTLOCATION = "CassandraSampler.inputlocation";
	public final static String DOREAD = "CassandraSampler.doRead";
	public final static String USERANDOMACCESS = "CassandraSampler.useRandomAccess";
	public final static String CHECKREAD = "CassandraSampler.checkRead";
	public final static String DOWRITE = "CassandraSampler.doWrite";
	
	
	private static final long serialVersionUID = 6427570355744485897L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	private static BinaryFileInfo binaryInfo;
	
	
	public CassandraSampler() {
		binaryInfo = null;
		trace("CassandraSampler() " + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		String threadName = Thread.currentThread().getName();
		int threadID =  Integer.parseInt(threadName.substring(threadName.length()-1));
		trace("sample() ThreadID: " + threadID);

		// Get the static instance of the BinaryFileInfo 
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());		
		
		// Get a QueryHandler instance.
		CassandraQueryHandler queryHandler = null;
		try {
			queryHandler = new CassandraQueryHandler(getDatabase(), getKeyspace());
		} catch (Exception e) {
			log.error("Failed to create a CassandraQueryHandler instance for the " + threadName + " sampler.");
		}				

		// Initialize the sampler and start it.
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
        res.sampleStart();
        
        if (Boolean.parseBoolean(getDoRead())) // DO THE READS IF THAT WAS REQUESTED.
        	readFromCassandra(queryHandler, res);
        else if (Boolean.parseBoolean(getDoWrite())) // DO THE WRITES IF IT WAS REQUESTED.
        	writeToCassandra(queryHandler, res);
        
        return res;
	}
	
	
	private void readFromCassandra(CassandraQueryHandler queryHandler, SampleResult res) {
		HashMap<String, String> hashes = null;
    	if (Boolean.parseBoolean(getUseRandomAccess()))
    		hashes = binaryInfo.getRandomHashesAndIDs();
    	else {
    		// Adding code element, where the IDs are not random generated, but defined by thread!
    	}
    	try {
    		byte[] result = 
    				queryHandler.readBinaryFromCassandra("TestCF", "data", hashes.get("original"), hashes.get("chunk"));
    		if (result == null)
    			throw new NotFoundInDBException("Row with the key not found in the database");
    		
    		if (Boolean.getBoolean(getCheckRead())) {
    			if (Boolean.parseBoolean(getCheckRead())) {
    				String filePath = 
    						binaryInfo.getBinaryFilePathList().get(hashes.get("originalID")).get(hashes.get("chunkID"));
    				byte[] fileContent = binaryInfo.read(filePath);
    				if (!Arrays.equals(result, fileContent))
    					CustomSamplerUtils.finalizeResponse(res, false, "500", "Read value is not correct!");
    			}
    		}
			
    		CustomSamplerUtils.finalizeResponse(res, true, "200",
					"Value read for:" + " B:" + hashes.get("originalID") + " C:" + hashes.get("chunkID") + " Success!");
			
    	} catch (Exception ex) {
    		log.error("Cassandra read attempt failed: ", ex);
    		CustomSamplerUtils.finalizeResponse(res, false, "500", ex.toString());
    	} finally {
    		res.sampleEnd();
    	}
	}
	
	private void writeToCassandra(CassandraQueryHandler queryHandler, SampleResult res) {
		HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
    	String originalID = hashes.get("originalID");
    	String chunkID = hashes.get("chunkID");
    	String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
    	byte[] fileContent = binaryInfo.read(filePath);
    	
    	try {
			queryHandler.writeBinaryToCassandra("TestCF", "data", hashes.get("original"), hashes.get("chunk"), fileContent);		
			CustomSamplerUtils.finalizeResponse(res, true, "200",
					"Value write for:" + " B:" + originalID + " C:" + chunkID + " Success!");
		} catch (CustomSamplersException ex) {
			log.error("Cassandra write attempt failed: ", ex);
			CustomSamplerUtils.finalizeResponse(res, false, "500", ex.toString());
		} finally {
			res.sampleEnd();
		}
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
	public String getKeyspace() {
		return getPropertyAsString(KEYSPACE);
	}
	public void setKeyspace(String keyspace) {
		setProperty(KEYSPACE, keyspace);
	}
	public String getInputLocation() {
		return getPropertyAsString(INPUTLOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUTLOCATION, inputLocation);
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

}
