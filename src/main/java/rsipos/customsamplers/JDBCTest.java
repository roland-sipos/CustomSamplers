package rsipos.customsamplers;

import java.io.ByteArrayInputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCTest {

	public static void main(String[] argv) {
		 
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
 
		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
 
		Connection connection = null;
 
		try {
			//connection = DriverManager.getConnection(
			//		"jdbc:postgresql://137.138.229.253:5432/testdb", "postgres", "testPass");
 
			connection = DriverManager.getConnection(
					"jdbc:mysql://137.138.229.253:3306/testdb", "root", "testPass");
			
			connection.setAutoCommit(false);
			
			//dropTable(connection);
			//Boolean ok = createTable(connection);
			Boolean ok = createMySQLTable(connection);
			
			//Boolean ok = createLOBTable(connection);
			//insertTable(connection);
			connection.commit();
			
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("You made it, take control your database now!");
	}

	public static Boolean insertTable(Connection connection) throws SQLException {
		byte[] value = "abcdefghijkl".getBytes();
		ByteArrayInputStream valueStream = new ByteArrayInputStream(value);
		
		PreparedStatement ps = connection.prepareStatement(
				"INSERT INTO binaries(hash, chunk_id, binary_id, data) VALUES (?, ?, ?, ?)");
		ps.setString(1, "brutallonghash");
		ps.setString(2, "chunk1");
		ps.setString(3, "binary1");
		ps.setBinaryStream(4, valueStream, value.length);
		Boolean success = ps.execute();
		System.out.println(success);
		ps.close();
		return success;
	}
	
	private static Boolean createMySQLTable(Connection connection) throws SQLException {
		PreparedStatement create = connection.prepareStatement(
				"CREATE TABLE binaries(" +
						"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
						"hash VARCHAR(225) NOT NULL UNIQUE," +
						"chunk_id VARCHAR(50) NOT NULL," +
						"binary_id VARCHAR(50) NOT NULL," +
		          		"data LONGBLOB)");
		Boolean success = create.execute();
		return success;
	}
	
	public static Boolean createTable(Connection connection) throws SQLException {
		PreparedStatement create = connection.prepareStatement(
		"CREATE TABLE binaries(" +
				"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
				"hash varchar(225) NOT NULL UNIQUE," +
				"chunk_id varchar(50) NOT NULL," +
				"binary_id varchar(50) NOT NULL," +
          		"data bytea)");

		Boolean success = create.execute();
		System.out.println(success.toString());
		return success;
	}
	
	public static Boolean createLOBTable(Connection connection) throws SQLException {
		PreparedStatement create = connection.prepareStatement(
				"CREATE TABLE binariesLO(" +
						"chunkOID OID," +
						"hash varchar(225) NOT NULL UNIQUE," +
						"chunk_id varchar(50) NOT NULL," +
						"binary_id varchar(50) NOT NULL)");

		Boolean success = create.execute();
		System.out.println(success.toString());
		return success;
	}
	
	public static Boolean dropTable(Connection connection) throws SQLException {
		PreparedStatement drop = connection.prepareStatement("DROP TABLE binaries");
		Boolean ok = drop.execute();
		System.out.println(ok.toString());
		return ok;
	}
	
	
}
