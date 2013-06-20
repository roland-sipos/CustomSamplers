package drizzle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
			byte[] fileContent, boolean isSpecial) throws CustomSamplersException {
		
		ByteArrayInputStream valueStream = new ByteArrayInputStream(fileContent);
		PreparedStatement ps;
		try {
			ps = connection.prepareStatement(
					"INSERT INTO "+ table + "(hash, chunk_id, binary_id, data) VALUES (?, ?, ?, ?)");
			ps.setString(1, hash);
			ps.setString(2, chunkID);
			ps.setString(3, binaryID);
			ps.setBinaryStream(4, valueStream, fileContent.length);
			ps.execute();
			ps.close();
			valueStream.close();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		} catch (IOException ie) {
			throw new CustomSamplersException("IOException occured during write attempt: " + ie.toString());
		}
		
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash,
			boolean isSpecial) throws CustomSamplersException {
		
		PreparedStatement ps;
		byte[] result = null;
		try {
			ps = connection.prepareStatement(
					"SELECT data FROM " + table + " WHERE hash=?");
			ps.setString(1, hash);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					result = rs.getBytes(1);
					counter++;
				}
				if (counter > 1)
					throw new CustomSamplersException(
							"More than one row found with hash=" + hash + " in " + table + " !");
				rs.close();
			} else {
				throw new CustomSamplersException("The row with hash=" + hash + " not found in the database!");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
		
	}

}
