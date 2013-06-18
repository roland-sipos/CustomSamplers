package drizzle;

import java.sql.Connection;

import customjdbc.CustomJDBCConfigElement;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class DrizzleQueryHandler implements QueryHandler  {

	private static Connection connection;
	private static String table;
	
	public DrizzleQueryHandler(String databaseName, String tableName) 
			throws CustomSamplersException, NotFoundInDBException {
		connection = CustomJDBCConfigElement.getJDBCConnection(databaseName);
		table = tableName;
		if (connection == null)
			throw new NotFoundInDBException("JDBCConnection instance with name: " + databaseName + " was not found in config!");
	}

	@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash,
			boolean isSpecial) throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

}
