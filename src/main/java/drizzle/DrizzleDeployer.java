package drizzle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utils.TestEnvironmentDeployer;

public class DrizzleDeployer {

	private static class DrizzleEnvironmentDeployer extends TestEnvironmentDeployer {

		Connection connection = null;
		
		public DrizzleEnvironmentDeployer(String host, String port,
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
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String createBinariesQuery = "DROP TABLE " + getEntity();
			PreparedStatement create = null;
			try {
				create = connection.prepareStatement(createBinariesQuery);
				Boolean failed = create.execute();
				if (!failed)
					System.out.println(" setupEnvironment() -> The following statement was successfull:\n" 
							+ createBinariesQuery);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
		
	};
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DrizzleEnvironmentDeployer deployer = 
				new DrizzleEnvironmentDeployer("testdb-pc.cern.ch", "4427", 
						"testdb", "binaries", "testUser", "testPass");
		
		System.out.println("-------- Drizzle environment setup ------------");
		deployer.deployTestEnv();
		//System.out.println("------- Drizzle environment teardown -----------");
		//deployer.destroyTestEnv();
		
	}

}
