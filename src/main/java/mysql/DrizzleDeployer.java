package mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.EnvironmentDeployer;

public class DrizzleDeployer {

	/**
	 * This class is the extended EnvironmentDeployer for Drizzle.
	 * */
	private static class DrizzleTestEnvironmentDeployer extends EnvironmentDeployer {

		private Connection connection = null;
		
		public DrizzleTestEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
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
	 
			System.out.println(" initialize() -> Drizzle JDBC Driver Registered!");
	 
			try {
				connection = DriverManager.getConnection("jdbc:drizzle://" + getHost() + ":" + getPort() 
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
					"chunkrow_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
					"hash VARCHAR(225) NOT NULL," +
					"chunk_id VARCHAR(50) NOT NULL, " +
					"binary_id VARCHAR(50) NOT NULL, " +	
	          		"data BLOB NOT NULL) ENGINE=InnoDB";
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
			String deleteBinariesQuery = "DROP TABLE BINARIES";
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
		DrizzleTestEnvironmentDeployer deployer = 
				new DrizzleTestEnvironmentDeployer("testdb-pc.cern.ch", "4427", 
						"testdb", "testUser", "testPass");
		
		/*
		 * Start the Drizzle server:
		 * e.g.:
		 * 	sudo /usr/local/sbin/drizzled --user=testUser --datadir=/opt/drizzle/data/ --basedir=/opt/drizzle/drizzle-7.1.36-stable/ --drizzle-protocol.bind-address=testdb-pc.cern.ch --config-dir=./
		 * 
		 * */
		
		System.out.println("-------- Drizzle environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- Drizzle environment teardown -----------");
		//deployer.destroyTestEnvironment();
		
	}

}
