package oracle;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import utils.TestEnvironmentDeployer;

public class OracleDeployer {

	/**
	 *This class is the extended TestEnvironmentDeployer for Oracle 11g.
	 */
	private static class OracleTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;

		public OracleTestEnvironmentDeployer(String host, String port,
				String databaseName, String entityName, String username, String password) {
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

			System.out.println(" initialize() -> Oracle JDBC Driver Registered!");

			try {
				connection = DriverManager.getConnection("jdbc:oracle:thin:@"
						+ getHost() + ":" + getPort() + ":" + getDatabase(), getUsername(), getPassword());
				connection.setAutoCommit(false);
			} catch (SQLException e) {
				System.out.println(" initialize() -> Connection Failed! "
						+ " Some parameter is not correct!");
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
			} catch (Exception e){
				System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
			}
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");

			String createTagQuery = "CREATE TABLE TAG ("
					+ " NAME VARCHAR2(100) NOT NULL,"
					+ " REVISION INT NULL,"
					+ " REVISION_TIME DATE NULL,"
					+ " COMMENTS VARCHAR2(4000) NULL,"
					+ " TIME_TYPE INT NULL,"
					+ " OBJECT_TYPE VARCHAR2(100) NULL,"
					+ " LAST_VALIDATED_TIME INT NULL,"
					+ " END_OF_VALIDITY INT NULL,"
					+ " LAST_SINCE INT NULL,"
					+ " LAST_SINCE_PID INT NULL,"
					+ " CREATION_TIME DATE NULL,"
					+ " PRIMARY KEY (NAME) )";
			String createPayloadQuery = "CREATE TABLE PAYLOAD ("
					+ " HASH VARCHAR(40) NOT NULL,"
					+ " OBJECT_TYPE VARCHAR(100) NULL,"
					+ " DATA BLOB NULL,"
					+ " STREAMER_INFO BLOB NULL,"
					+ " VERSION VARCHAR(20) NULL,"
					+ " CREATION_TIME DATE NULL,"
					+ " CMSSW_RELEASE VARCHAR(45) NULL,"
					+ " PRIMARY KEY (HASH) )";
			String createIOVQuery = "CREATE TABLE IOV ("
					+ " TAG_NAME VARCHAR(100) NOT NULL,"
					+ " SINCE INT NOT NULL,"
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " INSERT_TIME DATE NOT NULL,"
					+ " PRIMARY KEY (TAG_NAME, SINCE))";
			String createTagIdxQuery = "CREATE INDEX TAG_FK_IDX ON IOV (TAG_NAME ASC)";
			String createPayloadIdxQuery = "CREATE INDEX PAYLOAD_FK_IDX ON IOV (PAYLOAD_HASH ASC)";
			String alterIovTagFK = "ALTER TABLE IOV ADD CONSTRAINT TAG_FK_IDX "
					+ " FOREIGN KEY (TAG_NAME) REFERENCES TAG(NAME)";
			String alterIovPayloadFK = "ALTER TABLE IOV ADD CONSTRAINT PAYLOAD_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES PAYLOAD(HASH)";

			PreparedStatement create = null;
			String prefix = " setupEnvironment() -> ";
			try {
				create = connection.prepareStatement(createTagQuery);
				Boolean failed = create.execute();
				checkAndNotify(failed, createTagQuery, prefix);
				create = connection.prepareStatement(createPayloadQuery);
				failed = create.execute();
				checkAndNotify(failed, createPayloadQuery, prefix);
				create = connection.prepareStatement(createIOVQuery);
				failed = create.execute();
				checkAndNotify(failed, createIOVQuery, prefix);
				create = connection.prepareStatement(createTagIdxQuery);
				failed = create.execute();
				checkAndNotify(failed, createTagIdxQuery, prefix);
				create = connection.prepareStatement(createPayloadIdxQuery);
				failed = create.execute();
				checkAndNotify(failed, createPayloadIdxQuery, prefix);
				create = connection.prepareStatement(alterIovTagFK);
				failed = create.execute();
				checkAndNotify(failed, alterIovTagFK, prefix);
				create = connection.prepareStatement(alterIovPayloadFK);
				failed = create.execute();
				checkAndNotify(failed, alterIovPayloadFK, prefix);
				create.close();

				PreparedStatement insertTT = connection.prepareStatement("INSERT INTO TAG "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				insertTT.setString(1, "TEST_TAG");
				insertTT.setInt(2, 1);
				insertTT.setDate(3, new Date(System.currentTimeMillis()));
				insertTT.setString(4, "This is the first and only tag for testing.");
				insertTT.setInt(5, 1);
				insertTT.setString(6, "RANDOM");
				insertTT.setInt(7, 111);
				insertTT.setInt(8, 222);
				insertTT.setInt(9, 333);
				insertTT.setInt(10, 444);
				insertTT.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
				failed = insertTT.execute();
				checkAndNotify(failed, "INSERT INTO TAG statement", prefix);
				insertTT.close();

			} catch (SQLException e) {
				System.out.println(" setupEnvironment() -> SQLException occured. Details: " + e.toString());
			}

			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String deleteTagQuery = "DROP TABLE TAG CASCADE CONSTRAINTS";
			String deletePayloadQuery = "DROP TABLE PAYLOAD CASCADE CONSTRAINTS";
			String deleteIOVQuery = "DROP TABLE IOV CASCADE CONSTRAINTS";
			PreparedStatement delete = null;
			String prefix = " destroyEnvironment() -> ";
			try {
				delete = connection.prepareStatement(deleteIOVQuery);
				Boolean failed = delete.execute();
				checkAndNotify(failed, deleteIOVQuery, prefix);
				delete = connection.prepareStatement(deletePayloadQuery);
				failed = delete.execute();
				checkAndNotify(failed, deletePayloadQuery, prefix);
				delete = connection.prepareStatement(deleteTagQuery);
				failed = delete.execute();
				checkAndNotify(failed, deleteTagQuery, prefix);
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
		OracleTestEnvironmentDeployer deployer =
				new OracleTestEnvironmentDeployer("testdb-ora.cern.ch", "1521",
						"test", "binaries", "testUser", "testPass");

		//System.out.println("-------- Oracle environment setup ------------");
		//deployer.deployTestEnvironment();		
		//System.out.println("------- Oracle environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- Oracle environment teardown and setup ------------");
		deployer.redeployEnvironment();

	}

}

