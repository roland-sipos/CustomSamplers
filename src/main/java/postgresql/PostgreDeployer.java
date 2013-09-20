package postgresql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import utils.TestEnvironmentDeployer;

public class PostgreDeployer {

	/**
	 * This class is the extended TestEnvironmentDeployer for PostgreSQL.
	 * */
	private static class PostgreTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Connection connection = null;
		private Boolean useLargeObjectAPI = false;

		public PostgreTestEnvironmentDeployer(String host, String port, String databaseName,
				String username, String password, Boolean useLO) {
			super(host, port, databaseName, username, password);
			useLargeObjectAPI = useLO;
		}

		@Override
		protected void initialize() {
			/*try {
				Class.forName("org.postgresql.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println(" initialize() -> Where is your PostgreSQL JDBC Driver? "
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
			String createTagQuery = "CREATE TABLE TAG ("
					+ " NAME VARCHAR(100) NOT NULL,"
					+ " REVISION INT default NULL,"
					+ " REVISION_TIME TIMESTAMP default NULL,"
					+ " COMMENT VARCHAR(4000) default NULL,"
					+ " TIME_TYPE INT default NULL,"
					+ " OBJECT_TYPE VARCHAR(100) default NULL,"
					+ " LAST_VALIDATED_TIME INT default NULL,"
					+ " END_OF_VALIDITY INT default NULL,"
					+ " LAST_SINCE BIGINT default NULL,"
					+ " LAST_SINCE_PID INT default NULL,"
					+ " CREATION_TIME TIMESTAMP default NULL,"
					+ " PRIMARY KEY (NAME) )";

			String createLOBPayloadQuery = "CREATE TABLE LOB_PAYLOAD ("
					+ " HASH VARCHAR(40) NOT NULL ,"
					+ " OBJECT_TYPE VARCHAR(100) default NULL ,"
					+ " DATA OID default NULL ,"
					+ " STREAMER_INFO BYTEA default NULL ,"
					+ " VERSION VARCHAR(20) default NULL ,"
					+ " CREATION_TIME TIMESTAMP default NULL ,"
					+ " CMSSW_RELEASE VARCHAR(45) default NULL ,"
					+ " PRIMARY KEY (HASH) )";
			String createLOBChunkQuery = "CREATE TABLE LOB_CHUNK ("
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " CHUNK_HASH VARCHAR(40) NOT NULL,"
					+ " DATA OID default NULL,"
					+ " PRIMARY KEY (PAYLOAD_HASH, CHUNK_HASH) )";
			/*String LOBTriggerPayload = "CREATE TRIGGER t_lo_payload BEFORE " 
					+ " UPDATE OR DELETE ON LOB_PAYLOAD"
					+ " FOR EACH ROW EXECUTE PROCEDURE lo_manage(LOB_PAYLOAD)";
			String LOBTriggerChunk = "CREATE TRIGGER  t_lo_chunk BEFORE "
					+ " UPDATE OR DELETE ON LOB_CHUNK"
					+ " FOR EACH ROW EXECUTE PROCEDURE lo_manage(LOB_CHUNK)";*/

			String createPayloadQuery = "CREATE TABLE PAYLOAD ("
					+ " HASH VARCHAR(40) NOT NULL ,"
					+ " OBJECT_TYPE VARCHAR(100) default NULL ,"
					+ " DATA BYTEA default NULL ,"
					+ " STREAMER_INFO BYTEA default NULL ,"
					+ " VERSION VARCHAR(20) default NULL ,"
					+ " CREATION_TIME TIMESTAMP default NULL ,"
					+ " CMSSW_RELEASE VARCHAR(45) default NULL ,"
					+ " PRIMARY KEY (HASH) )";
			String createChunkQuery = "CREATE TABLE CHUNK ("
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " CHUNK_HASH VARCHAR(40) NOT NULL,"
					+ " DATA BYTEA default NULL,"
					+ " PRIMARY KEY (PAYLOAD_HASH, CHUNK_HASH) )";

			String createChunkIdxQuery = "CREATE INDEX PAYLOAD_HASH_FK_IDX ON CHUNK (PAYLOAD_HASH)";
			String createLOBChunkIdxQuery = "CREATE INDEX PAYLOAD_HASH_FK_IDX ON LOB_CHUNK (PAYLOAD_HASH)";

			String alterChunkHashFK = "ALTER TABLE CHUNK ADD CONSTRAINT PAYLOAD_HASH_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES PAYLOAD(HASH)";
			String alterLOBChunkHashFK = "ALTER TABLE LOB_CHUNK ADD CONSTRAINT PAYLOAD_HASH_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES LOB_PAYLOAD(HASH)";

			String createIOVQuery = "CREATE TABLE IOV ("
					+ " TAG_NAME VARCHAR(100) NOT NULL,"
					+ " SINCE BIGINT NOT NULL,"
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " INSERT_TIME TIMESTAMP NOT NULL,"
					+ " PRIMARY KEY (TAG_NAME, SINCE) )";
			String createTagIdxQuery = "CREATE INDEX TAG_FK_idx ON IOV (TAG_NAME)";
			String createPayloadIdxQuery = "CREATE INDEX PAYLOAD_FK_idx ON IOV (PAYLOAD_HASH)";

			String alterIovTagFK = "ALTER TABLE IOV ADD CONSTRAINT TAG_FK_IDX "
					+ " FOREIGN KEY (TAG_NAME) REFERENCES TAG(NAME)";
			String alterIovPayloadFK = "ALTER TABLE IOV ADD CONSTRAINT PAYLOAD_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES PAYLOAD(HASH)";
			String alterIovLOBPayloadFK = "ALTER TABLE IOV ADD CONSTRAINT PAYLOAD_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES LOB_PAYLOAD(HASH)";

			PreparedStatement create = null;
			String prefix = " setupEnvironment() -> ";
			try {
				create = connection.prepareStatement(createTagQuery);
				Boolean failed = create.execute();
				checkAndNotify(failed, createTagQuery, prefix);
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

				if (useLargeObjectAPI) {
					create = connection.prepareStatement(createLOBPayloadQuery);
					failed = create.execute();
					checkAndNotify(failed, createLOBPayloadQuery, prefix);
					create = connection.prepareStatement(createLOBChunkQuery);
					failed = create.execute();
					checkAndNotify(failed, createLOBChunkQuery, prefix);
					/*create = connection.prepareStatement(LOBTriggerPayload);
					failed = create.execute();
					checkAndNotify(failed, LOBTriggerPayload, prefix);
					create = connection.prepareStatement(LOBTriggerChunk);
					failed = create.execute();
					checkAndNotify(failed, LOBTriggerChunk, prefix);*/
					create = connection.prepareStatement(createLOBChunkIdxQuery);
					failed = create.execute();
					checkAndNotify(failed, createLOBChunkIdxQuery, prefix);
					create = connection.prepareStatement(alterLOBChunkHashFK);
					failed = create.execute();
					checkAndNotify(failed, alterLOBChunkHashFK, prefix);
					create = connection.prepareStatement(alterIovLOBPayloadFK);
					failed = create.execute();
					checkAndNotify(failed, alterIovLOBPayloadFK, prefix);
					
					
				} else {
					PreparedStatement createL = connection.prepareStatement(createPayloadQuery);
					failed = createL.execute();
					checkAndNotify(failed, createPayloadQuery, prefix);
					createL = connection.prepareStatement(createChunkQuery);
					failed = createL.execute();
					checkAndNotify(failed, createChunkQuery, prefix);
					createL = connection.prepareStatement(createChunkIdxQuery);
					failed = createL.execute();
					checkAndNotify(failed, createChunkIdxQuery, prefix);
					createL = connection.prepareStatement(alterChunkHashFK);
					failed = createL.execute();
					checkAndNotify(failed, alterChunkHashFK, prefix);
					createL = connection.prepareStatement(alterIovPayloadFK);
					failed = createL.execute();
					checkAndNotify(failed, alterIovPayloadFK, prefix);
					createL.close();
				}

				create.close();

				PreparedStatement insertTT = connection.prepareStatement("INSERT INTO TAG "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				insertTT.setString(1, "TEST_TAG");
				insertTT.setInt(2, 1);
				insertTT.setDate(3, new Date(System.currentTimeMillis()));
				insertTT.setString(4, "This is the first and only tag for testing.");
				insertTT.setInt(5, 1);
				insertTT.setString(6, "any_obj_type");
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
			String deleteTagQuery = "DROP TABLE IF EXISTS TAG";
			String deleteChunkQuery = "DROP TABLE IF EXISTS CHUNK";
			String deletePayloadQuery = "DROP TABLE IF EXISTS PAYLOAD";
			String deleteIOVQuery = "DROP TABLE IF EXISTS IOV";
			/*String truncateLOBChunkQuery = "DELETE * FROM LOB_CHUNK";
			String truncateLOBPayloadQuery = "DELETE * FROM LOB_PAYLOAD";*/
			String deleteLOBChunkQuery = "DROP TABLE IF EXISTS LOB_CHUNK";
			String deleteLOBPayloadQuery = "DROP TABLE IF EXISTS LOB_PAYLOAD";
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

				if (useLargeObjectAPI) {
					/*delete = connection.prepareStatement(truncateLOBChunkQuery);
					failed = delete.execute();
					checkAndNotify(failed, truncateLOBChunkQuery, prefix);
					delete = connection.prepareStatement(truncateLOBPayloadQuery);
					failed = delete.execute();
					checkAndNotify(failed, truncateLOBPayloadQuery, prefix);*/
					delete = connection.prepareStatement(deleteLOBChunkQuery);
					failed = delete.execute();
					checkAndNotify(failed, deleteLOBChunkQuery, prefix);
					delete = connection.prepareStatement(deleteLOBPayloadQuery);
					failed = delete.execute();
					checkAndNotify(failed, deleteLOBPayloadQuery, prefix);
				}

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
		PostgreTestEnvironmentDeployer deployer =
				new PostgreTestEnvironmentDeployer("testdb-pc.cern.ch", "5432", 
						"testdb", "postgres", "testPass", true); // Don't use LO API.

		//System.out.println("-------- PostgreSQL environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- PostgreSQL environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- PostgreSQL environment teardown and setup ------------");
		deployer.redeployEnvironment();

	}

}
