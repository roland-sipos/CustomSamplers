package rsipos.customsamplers;

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
			connection = DriverManager.getConnection(
					"jdbc:postgresql://137.138.229.253:5432/testdb", "postgres", "testPass");
 
			connection.setAutoCommit(false);
			
			PreparedStatement create = connection.prepareStatement(
					"CREATE TABLE binaries(" +
							"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
							"hash varchar(225) NOT NULL UNIQUE," +
			          		"data bytea)");
			
			//ResultSet res = create.executeQuery();
			Boolean success = create.execute();
			System.out.println(success.toString());
			
			connection.commit();
			
			/*PreparedStatement drop = connection.prepareStatement("DROP TABLE binaries");
			Boolean ok = drop.execute();
			System.out.println(ok.toString());
			connection.commit();*/
			
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}
 
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
	}

}
