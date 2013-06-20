package postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.TestEnvironmentDeployer;

public class PostgreDeployer {

	/**
	 * This class is the extended TestEnvironmentDeployer for PostgreSQL.
	 * */
	private static class PostgreTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;
		
		public PostgreTestEnvironmentDeployer(String host, String port,
				String databaseName, String entityName, String username, String password) {
			super(host, port, databaseName, entityName, username, password);
		}

		@Override
		protected void initialize() {
			
			/*try {
				Class.forName("org.drizzle.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println(" initialize() -> Where is your Drizzle JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
			}*/
 
			System.out.println(" initialize() -> PostgreSQL JDBC Driver Registered!");
 
			try {
				connection = DriverManager.getConnection("jdbc:postgresql://" + getHost() + ":" + getPort() 
						+ "/" + getDatabase(), getUsername(), getPassword());
				connection.setAutoCommit(false);
			} catch (SQLException e) {
				System.out.println(" initialize() -> Connection Failed! Some parameter is not correct!");
				e.printStackTrace();
				return;
			}
			System.out.println(" initialize() -> Connection established...\n");			
		}

		@Override
		protected void tearDown() {
			try {
	        	if (connection != null)
	        		connection.close();
	        	System.out.println(" tearDown() -> Connection closed.\n");
	        }
	        catch (Exception e){
	        	System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
	        }
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");
			
			String createBinariesQuery = "CREATE TABLE IF NOT EXISTS " + getEntity() + "(" +
					"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
					"hash varchar(225) NOT NULL UNIQUE," +
					"chunk_id varchar(50) NOT NULL," +
					"binary_id varchar(50) NOT NULL," +
	          		"data bytea)";
			String createLOBBinaryQuery = "CREATE TABLE IF NOT EXISTS " + getEntity() + "LO(" +
					"chunkOID OID," +
					"hash varchar(225) NOT NULL UNIQUE," +
					"chunk_id varchar(50) NOT NULL," +
					"binary_id varchar(50) NOT NULL)";
			PreparedStatement create = null;
			try {
				create = connection.prepareStatement(createBinariesQuery);
				Boolean failed = create.execute();
				if (!failed)
					System.out.println(" setupEnvironment() -> The following statement was successfull:\n" 
							+ createBinariesQuery);
				create = connection.prepareStatement(createLOBBinaryQuery);
				failed = create.execute();
				if (!failed)
					System.out.println(" setupEnvironment() -> The following statement was successfull:\n"
							+ createLOBBinaryQuery);
				create.close();
			} catch (SQLException e) {
				System.out.println(" setupEnvironment() -> SQLException occured. Details: " + e.toString());
			}
			
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String deleteBinariesQuery = "DROP TABLE " + getEntity();
			String deleteLOBBinariesQuery = "DROP TABLE " + getEntity() + "LO";
			PreparedStatement delete = null;
			try {
				delete = connection.prepareStatement(deleteBinariesQuery);
				Boolean failed = delete.execute();
				if (!failed)
					System.out.println(" destroyEnvironment() -> The following statement was successfull:\n" 
							+ deleteBinariesQuery);
				delete = connection.prepareStatement(deleteLOBBinariesQuery);
				failed = delete.execute();
				if (!failed)
					System.out.println(" destroyEnvironment() -> The following statement was successfull:\n" 
							+ deleteBinariesQuery);
				delete.close();
			} catch (SQLException e) {
				System.out.println(" destroyEnvironment() -> SQLException occured. Details: " + e.toString());
			}
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PostgreTestEnvironmentDeployer deployer =
				new PostgreTestEnvironmentDeployer("testdb-pc.cern.ch", "5432", 
						"testdb", "binaries", "postgres", "testPass");
	    
		System.out.println("-------- PostgreSQL environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- PostgreSQL environment teardown -----------");
		//deployer.destroyTestEnvironment();

	}

}
