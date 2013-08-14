package oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.TestEnvironmentDeployer;

public class Oracle11gXEDeployer {

	/**
	 * This class is the extended TestEnvironmentDeployer for MySQL.
	 * */
	private static class Oracle11gXETestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;
		
		public Oracle11gXETestEnvironmentDeployer(String host, String port,
				String databaseName, String entityName, String username,
				String password) {
			super(host, port, databaseName, entityName, username, password);
		}

		@Override
		protected void initialize() {
			/*try {
				Class.forName("com.oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException e) {
				System.out.println(" initialize() -> Where is your Oracle 11g XE JDBC Driver? "
				+ "Include in your library path!");
				e.printStackTrace();
				return;
			}*/
            
			System.out.println(" initialize() -> Oracle 11g XE JDBC Driver Registered!");

			try {
				connection = DriverManager.getConnection("jdbc:oracle:thin@" + getHost() + ":" + getPort() 
						+ ":" + getDatabase(), getUsername(), getPassword());
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
			
			/*String createBinariesQuery = "CREATE TABLE IF NOT EXISTS binaries (" +
					"chunkrow_id SERIAL NOT NULL PRIMARY KEY," +
					"hash VARCHAR(225) NOT NULL UNIQUE," +
					"chunk_id VARCHAR(50) NOT NULL," +
					"binary_id VARCHAR(50) NOT NULL," +
	          		"data LONGBLOB)";
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
			}*/
			
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			/*String deleteBinariesQuery = "DROP TABLE " + getEntity();
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
			}*/
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Oracle11gXETestEnvironmentDeployer deployer =
				new Oracle11gXETestEnvironmentDeployer("node-test.cern.ch", "1521", 
						"testdb", "binaries", "testUser", "testPass");
	    
		System.out.println("-------- Oracle 11g XE environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- Oracle 11g XE environment teardown -----------");
		//deployer.destroyTestEnvironment();
	}

}
