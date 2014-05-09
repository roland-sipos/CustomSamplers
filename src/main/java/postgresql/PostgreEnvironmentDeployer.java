package postgresql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import utils.EnvironmentDeployer;

/**
 * This class is the extended EnvironmentDeployer for PostgreSQL.
 * */
class PostgreEnvironmentDeployer extends EnvironmentDeployer {

	private Connection connection = null;
	private List<String> tagList;

	public PostgreEnvironmentDeployer(String host, String port,
			String databaseName, String username, String password, List<String> tags) {
		super(host, port, databaseName, username, password);
		tagList = tags;
	}

	@Override
	protected void initialize() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println(" initialize() -> Where is your PostgreSQL JDBC Driver? "
				+ "Include it in your library path!");
		e.printStackTrace();
		return;
		}
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
				+ " ID SMALLINT NOT NULL,"
				+ " DATA BYTEA default NULL,"
				+ " PRIMARY KEY (PAYLOAD_HASH, CHUNK_HASH) )";

		String createChunkIdxQuery = "CREATE INDEX PAYLOAD_HASH_FK_IDX ON CHUNK (PAYLOAD_HASH)";

		String alterChunkHashFK = "ALTER TABLE CHUNK ADD CONSTRAINT PAYLOAD_HASH_FK_IDX "
				+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES PAYLOAD(HASH)";

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

			create.close();

			for (int i = 0; i < tagList.size(); ++i) {
				PreparedStatement insertTT = connection.prepareStatement("INSERT INTO TAG "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				insertTT.setString(1, tagList.get(i));
				insertTT.setInt(2, 1);
				insertTT.setDate(3, new Date(System.currentTimeMillis()));
				insertTT.setString(4, "This is the tag for " + tagList.get(i)+ ".");
				insertTT.setInt(5, 1);
				insertTT.setString(6, "any_obj_type");
				insertTT.setInt(7, 111);
				insertTT.setInt(8, 222);
				insertTT.setInt(9, 333);
				insertTT.setInt(10, 444);
				insertTT.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
				failed = insertTT.execute();
				checkAndNotify(failed, "INSERT INTO TAG statement - " + tagList.get(i), prefix);
				insertTT.close();
			}

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

