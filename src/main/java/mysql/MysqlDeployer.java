package mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.TestEnvironmentDeployer;

public class MysqlDeployer {

	/**
	 * This class is the extended TestEnvironmentDeployer for MySQL.
	 * */
	private static class MysqlTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;
		private String whichEngine = null;
		
		public MysqlTestEnvironmentDeployer(String host, String port,
				String databaseName, String entityName, String username,
				String password, String whichEngine) {
			super(host, port, databaseName, entityName, username, password);
			this.whichEngine = whichEngine;
		}

		@Override
		protected void initialize() {
			/*try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println(" initialize() -> Where is your MySQL JDBC Driver? "
					+ "Include in your library path!");
				e.printStackTrace();
				return;
			}*/
 
			System.out.println(" initialize() -> MySQL JDBC Driver Registered!");
 
			try {
				connection = DriverManager.getConnection("jdbc:mysql://" + getHost() + ":" + getPort() 
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
			
			String createBinariesQuery = "CREATE TABLE IF NOT EXISTS binaries (" +
					"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
					"hash VARCHAR(225) NOT NULL UNIQUE," +
					"chunk_id VARCHAR(50) NOT NULL," +
					"binary_id VARCHAR(50) NOT NULL," +
	          		"data LONGBLOB) ENGINE=" + whichEngine;
			PreparedStatement create = null;
			try {
				create = connection.prepareStatement(createBinariesQuery);
				Boolean failed = create.execute();
				if (!failed)
					System.out.println(" setupEnvironment() -> The following statement was successfull:\n" 
							+ createBinariesQuery);
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
			PreparedStatement delete = null;
			try {
				delete = connection.prepareStatement(deleteBinariesQuery);
				Boolean failed = delete.execute();
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
		MysqlTestEnvironmentDeployer deployer =
				new MysqlTestEnvironmentDeployer("testdb-pc.cern.ch", "3306", 
						"testdb", "binaries", "testUser", "testPass", "InnoDB");
	    
		System.out.println("-------- MySQL environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- MySQL environment teardown -----------");
		//deployer.destroyTestEnvironment();
		
	}
	

}
