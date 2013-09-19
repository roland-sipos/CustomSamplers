package mysql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import utils.TestEnvironmentDeployer;

public class MysqlDeployer {

	/**
	 * This class is the extended TestEnvironmentDeployer for MySQL.
	 * */
	private static class MysqlTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;
		private String whichEngine = null;

		public MysqlTestEnvironmentDeployer(String host, String port, String databaseName,
				String username, String password, String whichEngine) {
			super(host, port, databaseName, username, password);
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
				if (connection != null) {
					connection.close();
				}
				System.out.println(" tearDown() -> Connection closed.\n");
			} catch (Exception e) {
				System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
			}
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");

			String createTagQuery = "CREATE  TABLE IF NOT EXISTS `TAG` ("
					+ " `NAME` VARCHAR(100) NOT NULL,"
					+ " `REVISION` INT NULL,"
					+ " `REVISION_TIME` DATETIME NULL,"
					+ " `COMMENT` VARCHAR(4000) NULL,"
					+ " `TIME_TYPE` INT NULL,"
					+ " `OBJECT_TYPE` VARCHAR(100) NULL,"
					+ " `LAST_VALIDATED_TIME` INT NULL,"
					+ " `END_OF_VALIDITY` INT NULL,"
					+ " `LAST_SINCE` BIGINT NULL,"
					+ " `LAST_SINCE_PID` INT NULL,"
					+ " `CREATION_TIME` TIMESTAMP NULL,"
					+ " PRIMARY KEY (`NAME`) )"
					+ " ENGINE = " + whichEngine;

			String createPayloadQuery = "CREATE  TABLE IF NOT EXISTS `PAYLOAD` ("
					+ " `HASH` VARCHAR(40) NOT NULL ,"
					+ " `OBJECT_TYPE` VARCHAR(100) NULL ,"
					+ " `DATA` LONGBLOB NULL ,"
					+ " `STREAMER_INFO` BLOB NULL ,"
					+ " `VERSION` VARCHAR(20) NULL ,"
					+ " `CREATION_TIME` TIMESTAMP NULL ,"
					+ " `CMSSW_RELEASE` VARCHAR(45) NULL ,"
					+ " PRIMARY KEY (`HASH`) )"
					+ " ENGINE = " + whichEngine;

			String createChunkQuery = "CREATE TABLE IF NOT EXISTS `CHUNK` ("
					+ " `PAYLOAD_HASH` VARCHAR(40) NOT NULL,"
					+ " `CHUNK_HASH` VARCHAR(40) NOT NULL,"
					+ " `DATA` LONGBLOB NULL,"
					+ " PRIMARY KEY (`PAYLOAD_HASH`, `CHUNK_HASH`) )"
					+ " ENGINE = " + whichEngine;
			String createChunkIdxQuery = "CREATE INDEX `PAYLOAD_HASH_FK_IDX` ON `CHUNK` (`PAYLOAD_HASH` ASC)";
			String alterChunkHashFK = "ALTER TABLE `CHUNK` ADD CONSTRAINT `PAYLOAD_HASH_FK_IDX` "
					+ " FOREIGN KEY (`PAYLOAD_HASH`) REFERENCES `PAYLOAD`(`HASH`)";

			String createIOVQuery = "CREATE TABLE IF NOT EXISTS `IOV` ("
					+ " `TAG_NAME` VARCHAR(100) NOT NULL,"
					+ " `SINCE` BIGINT NOT NULL,"
					+ " `PAYLOAD_HASH` VARCHAR(40) NOT NULL,"
					+ " `INSERT_TIME` DATETIME NOT NULL,"
					+ " PRIMARY KEY (`TAG_NAME`, `SINCE`),"
					+ "CONSTRAINT `TAG_FK_idx`"
					+ " FOREIGN KEY (`TAG_NAME` ) REFERENCES `TAG` (`NAME` ),"
					+ " FOREIGN KEY (`PAYLOAD_HASH`) REFERENCES `PAYLOAD` (`HASH`) )"
					+ "ENGINE = " + whichEngine;

			String createTagIdxQuery = "CREATE INDEX `TAG_FK_idx` ON `IOV` (`TAG_NAME` ASC)";
			String createPayloadIdxQuery = "CREATE INDEX `PAYLOAD_FK_idx` ON `IOV` (`PAYLOAD_HASH` ASC)";

			PreparedStatement create = null;
			String prefix = " setupEnvironment() -> ";
			try {
				create = connection.prepareStatement(createTagQuery);
				Boolean failed = create.execute();
				checkAndNotify(failed, createTagQuery, prefix);
				create = connection.prepareStatement(createPayloadQuery);
				failed = create.execute();
				checkAndNotify(failed, createPayloadQuery, prefix);
				create = connection.prepareStatement(createChunkQuery);
				failed = create.execute();
				checkAndNotify(failed, createChunkQuery, prefix);
				create = connection.prepareStatement(createChunkIdxQuery);
				failed = create.execute();
				checkAndNotify(failed, createChunkIdxQuery, prefix);
				create = connection.prepareStatement(alterChunkHashFK);
				failed = create.execute();
				checkAndNotify(failed, alterChunkHashFK, prefix);
				create = connection.prepareStatement(createIOVQuery);
				failed = create.execute();
				checkAndNotify(failed, createIOVQuery, prefix);
				create = connection.prepareStatement(createTagIdxQuery);
				failed = create.execute();
				checkAndNotify(failed, createTagIdxQuery, prefix);
				create = connection.prepareStatement(createPayloadIdxQuery);
				failed = create.execute();
				checkAndNotify(failed, createPayloadIdxQuery, prefix);
				create.close();

				PreparedStatement insertTT = connection.prepareStatement("INSERT INTO `TAG` "
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

				connection.commit();

			} catch (SQLException e) {
				System.out.println(" setupEnvironment() -> SQLException occured. Details: " + e.toString());
			}
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String deleteTagQuery = "DROP TABLE IF EXISTS `TAG`";
			String deleteChunkQuery = "DROP TABLE IF EXISTS `CHUNK`";
			String deletePayloadQuery = "DROP TABLE IF EXISTS `PAYLOAD`";
			String deleteIOVQuery = "DROP TABLE IF EXISTS `IOV`";
			PreparedStatement delete = null;
			String prefix = " destroyEnvironment() -> ";
			try {
				delete = connection.prepareStatement(deleteIOVQuery);
				Boolean failed = delete.execute();
				checkAndNotify(failed, deleteIOVQuery, prefix);
				delete = connection.prepareStatement(deleteChunkQuery);
				failed = delete.execute();
				checkAndNotify(failed, deleteChunkQuery, prefix);
				delete = connection.prepareStatement(deletePayloadQuery);
				failed = delete.execute();
				checkAndNotify(failed, deletePayloadQuery, prefix);
				delete = connection.prepareStatement(deleteTagQuery);
				failed = delete.execute();
				checkAndNotify(failed, deleteTagQuery, prefix);
				delete.close();

				connection.commit();

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
						"testdb", "testUser", "testPass", "InnoDB");

		//System.out.println("-------- MySQL environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- MySQL environment teardown ----------");
		//deployer.destroyTestEnvironment();
		System.out.println("------- MySQL environment teardown and setup -----------");
		deployer.redeployEnvironment();

	}

}

