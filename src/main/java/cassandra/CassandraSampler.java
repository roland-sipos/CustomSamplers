/**
 * 
 */
package cassandra;

import java.util.HashMap;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;
import utils.CustomSamplersException;
import utils.NotFoundInDBException;

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
		// Get the static instance of the BinaryFileInfo 
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
		String threadName = Thread.currentThread().getName();

		CassandraQueryHandler queryHandler = null;
		try {
			queryHandler = new CassandraQueryHandler(getDatabase(), getKeyspace());
		} catch (Exception e) {
			log.error("Failed to create a CassandraQueryHandler instance for the " + threadName + " sampler.");
		}				
		
		int threadID =  Integer.parseInt(threadName.substring(threadName.length()-1));
		trace("sample() ThreadID: " + threadID);
		
		SampleResult res = CustomSamplerUtils.getInitialSampleResult(getTitle());
        res.sampleStart();
        
        if (Boolean.parseBoolean(getDoRead())) { // DO THE READS IF THAT WAS REQUESTED.
        	HashMap<String, String> hashes = null;
        	if (Boolean.parseBoolean(getUseRandomAccess())) {
        		hashes = binaryInfo.getRandomHashesAndIDs();
        	} else {
        		// Adding code element, where the IDs are not random generated, but defined by thread!
        	}
			
        	Composite key = new Composite();
        	key.addComponent(hashes.get("original"), StringSerializer.get());
        	key.addComponent(hashes.get("chunk"), StringSerializer.get());
        	try {
        		byte[] result = queryHandler.readBinaryFromCassandra("TestCF", "data", key);
				
        		if (result == null)
        			throw new NotFoundInDBException("Row with the key not found in the database");
        		
        		if (Boolean.getBoolean(getCheckRead())) {
        			// Check the value equivalence between read data and file content!
        		}
				
        		res.latencyEnd();			
        		String responseStr = "Value found for:" 
        					+ " B:" + hashes.get("originalID")
        					+ " C:" + hashes.get("chunkID") + " Success!"; 
        		res.setResponseData(responseStr.getBytes());
				
        	} catch (Exception ex) {
        		log.warn("", ex);
        		res.setResponseCode("500");
        		res.setSuccessful(false);
        		res.setResponseMessage(ex.toString());
        		res.setResponseData(ex.getMessage().getBytes());
        	} finally {
        		res.sampleEnd();
        	}
        
        } else if (Boolean.parseBoolean(getDoWrite())) { // DO THE WRITES IF IT WAS REQUESTED.
        	HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
        	String originalID = hashes.get("originalID");
        	String chunkID = hashes.get("chunkID");
        	String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
        	byte[] fileContent = binaryInfo.read(filePath);
        	
        	Composite key = new Composite();
        	key.addComponent(hashes.get("original"), StringSerializer.get());
        	key.addComponent(hashes.get("chunk"), StringSerializer.get());
        	try {
				queryHandler.writeBinaryToCassandra("TestCF", "data", key, fileContent);
				res.latencyEnd();			
				String responseStr = "Value found for:" + " B:" + originalID + " C:" + chunkID + " Success!";
	    		res.setResponseData(responseStr.getBytes());
			} catch (CustomSamplersException ex) {
				log.warn("", ex);
        		res.setResponseCode("500");
        		res.setSuccessful(false);
        		res.setResponseMessage(ex.toString());
        		res.setResponseData(ex.getMessage().getBytes());
			} finally {
				res.sampleEnd();
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
