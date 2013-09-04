package postgresql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import customjdbc.CustomJDBCConfigElement;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class PostgreQueryHandler implements QueryHandler {

	private static Connection connection;
	private static String table;
	private static String lobTable;
	
	public PostgreQueryHandler(String databaseName, String tableName) 
			throws CustomSamplersException, NotFoundInDBException {
		connection = CustomJDBCConfigElement.getJDBCConnection(databaseName);
		table = tableName;
		lobTable = tableName + "LO"; 
		if (connection == null)
			throw new NotFoundInDBException("JDBCConnection instance with name: " + databaseName + " was not found in config!");
	}
	
	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial) 
			throws CustomSamplersException {
		if (isSpecial)
			return readLOBFromPostgre(hash);
		
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
			} else
				throw new CustomSamplersException("The row with hash=" + hash + " not found in the database!");
			
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}
	
	@Override
	public void writeBinary(String binaryID, String chunkID, String hash, 
			                byte[] fileContent, boolean isSpecial) 
			throws CustomSamplersException {
		if (isSpecial) {
			writeLOBToPostgre(binaryID, chunkID, hash, fileContent);
			return;
		}
		
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

	public void writeLOBToPostgre(String binaryID, String chunkID, String hash, byte[] fileContent) 
			throws CustomSamplersException {
		try {
			connection.setAutoCommit(false);
			LargeObjectManager lobManager = 
					((org.postgresql.PGConnection)connection).getLargeObjectAPI();
			long objectId = lobManager.createLO(LargeObjectManager.READWRITE);
			LargeObject object = lobManager.open(objectId, LargeObjectManager.WRITE);
			object.write(fileContent);
			object.close();
			
			PreparedStatement ps = connection.prepareStatement(
					"INSERT INTO " + lobTable + "VALUES (?, ?, ?, ?)");
			ps.setLong(1, objectId);
			ps.setString(2, hash);
			ps.setString(3, chunkID);
			ps.setString(4, binaryID);
			ps.executeUpdate();
			ps.close();
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during LOB insert attempt: " + e.toString());
		}
	}

	public byte[] readLOBFromPostgre(String hash) 
			throws CustomSamplersException {
		byte[] result = null;
		try {
			LargeObjectManager lobManager = 
					((org.postgresql.PGConnection)connection).getLargeObjectAPI();
			PreparedStatement ps = connection.prepareStatement(
						"SELECT imgOID FROM " + lobTable + "WHERE hash=?");
			ps.setString(1, hash);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					result = new byte[object.size()];
					object.read(result, 0, object.size());
					object.close();
					counter++;
				}
				if (counter > 1) 
					throw new CustomSamplersException(
							"More than one row found with hash=" + hash + " in " + lobTable + " !");
				rs.close();
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during LOB read attemtp: " + e.toString());
		}
		return result;
	}

	@Override
	public void writePayload(HashMap<String, String> metaMap, byte[] payload,
			byte[] streamerInfo, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readPayload(HashMap<String, String> metaMap, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
