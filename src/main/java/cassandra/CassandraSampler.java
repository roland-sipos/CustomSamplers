/**
 * 
 */
package cassandra;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryconfig.BinaryConfigElement;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

/**
 * @author cb
 *
 */
public class CassandraSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 6427570355744485897L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "CassandraSampler.database";
	public final static String KEYSPACE = "CassandraSampler.keyspace";
    public final static String COLUMNFAMILY = "CassandraSampler.columnFamily";
	public final static String BINARYINFO = "CassandraSampler.binaryInfo";
	public final static String DOREAD = "CassandraSampler.doRead";
	public final static String USERANDOMACCESS = "CassandraSampler.useRandomAccess";
	public final static String CHECKREAD = "CassandraSampler.checkRead";
	public final static String DOWRITE = "CassandraSampler.doWrite";
	public final static String ASSIGNED_WRITE = "CassandraSampler.assignedWrite";

	
	public CassandraSampler() {
		trace("CassandraSampler() " + this.toString());
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		BinaryFileInfo binaryInfo = null;
		CassandraQueryHandler queryHandler = null;
		try {
			binaryInfo = BinaryConfigElement.getBinaryFileInfo(getBinaryInfo());
			queryHandler = new CassandraQueryHandler(getDatabase(), getKeyspace(), getColumnFamily());
		} catch (Exception e) {
			log.error("Failed to create a CassandraQueryHandler instance for the " + 
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
	
	
	/*private void readFromCassandra(CassandraQueryHandler queryHandler, SampleResult res) {
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
	}*/
	
	/*private void writeToCassandra(CassandraQueryHandler queryHandler, SampleResult res) {
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
	}*/
	
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
	public String getColumnFamily() {
		return getPropertyAsString(COLUMNFAMILY);
	}
	public void setColumnFamily(String columnFamily) {
		setProperty(COLUMNFAMILY, columnFamily);
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
