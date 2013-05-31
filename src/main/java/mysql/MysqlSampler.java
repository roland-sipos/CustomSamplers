package mysql;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplerUtils;

public class MysqlSampler extends AbstractSampler implements TestBean {

	private static final long serialVersionUID = 3170361822373773213L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String DATABASE = "MysqlSampler.database";
	public final static String TABLE = "MysqlSampler.table";
	public final static String INPUTLOCATION = "MysqlSampler.inputlocation";
	public final static String DOREAD = "MysqlSampler.doRead";
	public final static String USERANDOMACCESS = "MysqlSampler.useRandomAccess";
	public final static String CHECKREAD = "MysqlSampler.checkRead";
	public final static String DOWRITE = "MysqlSampler.doWrite";
	public final static String ASSIGNED_WRITE = "MysqlSampler.assignedWrite";

	public static BinaryFileInfo binaryInfo;

	
	public MysqlSampler() {
		binaryInfo = null;
		trace("MysqlSampler()" + this.toString());
	}

	@Override
	public SampleResult sample(Entry arg0) {
		int threadID = CustomSamplerUtils.getThreadID(Thread.currentThread().getName());
		trace("sample() ThreadID: " + threadID);
		
		// Get BinaryInfo and QueryHandler instances.
		binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
		MysqlQueryHandler queryHandler = null;
		try {
			queryHandler = new MysqlQueryHandler(getDatabase(), getTable());
		} catch (Exception e) {
			log.error("Failed to create a PostgreQueryHandler instance for the " + 
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
	public String getTable() {
		return getPropertyAsString(TABLE);
	}
	public void setTable(String table) {
		setProperty(TABLE, table);
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
	public String getAssignedWrite() {
		return getPropertyAsString(ASSIGNED_WRITE);
	}
	public void setAssignedWrite(String assignedWrite) {
		setProperty(ASSIGNED_WRITE, assignedWrite);
	}
	
}