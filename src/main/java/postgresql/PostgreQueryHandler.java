package postgresql;

import java.sql.Connection;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;

import customjdbc.CustomJDBCConfigElement;

public class PostgreQueryHandler {

	private static Connection connection;
	
	public PostgreQueryHandler(String databaseName) 
			throws CustomSamplersException, NotFoundInDBException {
		connection = CustomJDBCConfigElement.getJDBCConnection(databaseName);
		if (connection == null)
			throw new NotFoundInDBException("JDBC connection instance with name: " + databaseName + " was not found in config!");		
	}
	
	public byte[] readFromPostgre(String binaryID, String chunkID, String hash)
			throws CustomSamplersException {
		return null;
	}
	
	public void writeToPostgre(String binaryID, String chunkID, String hash, byte[] fileContent) 
			throws CustomSamplersException {
		
	}
	
}
