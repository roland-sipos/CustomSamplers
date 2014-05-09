package oracle;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import utils.DeployerOptions;
import utils.TagList;
import utils.EnvironmentDeployer;

/** 
 * This executable class is capable to deploy an Oracle schema into a database instance.
 * */
public class OracleDeployer {

	/**
	 *This class is the extended EnvironmentDeployer for Oracle 11g.
	 */
	private static class OracleEnvironmentDeployer extends EnvironmentDeployer {

		/** The JDBC Connection object. */
		private Connection connection = null;
		/** Tag list to fill now. */
		private List<String> tagList;

		/** Constructor
		 * @param host target host for deployment (passed for super EnvironmentDeployer)
		 * @param port port of target host (passed for super)
		 * @param databaseName the name of the database instance on target host (passed for super)
		 * @param username the user's name for target authentication (passed for super)
		 * @param password the user's password for target authentication (passed for super)
		 * @param tags possible TAGs will be written into the database directly. (transaction specific)
		 * */
		public OracleEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password, List<String> tags) {
			super(host, port, databaseName, username, password);
			tagList = tags;
		}

		/** Initialize function for Oracle. */
		@Override
		protected void initialize() {
			/** A fast-check for the JDBC driver. */
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException e) {
				System.out.println(" initialize() -> Where is your Oracle JDBC Driver? "
					+ "Include in your library path!");
				e.printStackTrace();
				return;
			}
			System.out.println(" initialize() -> Oracle JDBC Driver Registered!");

			try {
				connection = DriverManager.getConnection("jdbc:oracle:thin:@"
						+ getHost() + ":" + getPort() + ":" + getDatabase(), getUsername(), getPassword());
				//connection.setAutoCommit(false);
			} catch (SQLException e) {
				System.out.println(" initialize() -> Connection Failed! "
						+ " Some parameter is not correct!");
				e.printStackTrace();
				return;
			}
			System.out.println(" initialize() -> Connection established...\n");	
		}

		/** Tear-down function for Oracle. */
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

		/** Environment setup function for Oracle. */
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

			String createChunkQuery = "CREATE TABLE CHUNK ("
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " CHUNK_HASH VARCHAR(40) NOT NULL,"
					+ " ID SMALLINT NOT NULL,"
					+ " DATA BLOB NULL,"
					+ " PRIMARY KEY (PAYLOAD_HASH, CHUNK_HASH) )";
			String createChunkIdxQuery = "CREATE INDEX PAYLOAD_HASH_FK_IDX ON CHUNK (PAYLOAD_HASH ASC)";
			String alterChunkHashFK = "ALTER TABLE CHUNK ADD CONSTRAINT PAYLOAD_HASH_FK_IDX "
					+ " FOREIGN KEY (PAYLOAD_HASH) REFERENCES PAYLOAD(HASH)";

			String createIOVQuery = "CREATE TABLE IOV ("
					+ " TAG_NAME VARCHAR(100) NOT NULL,"
					+ " SINCE INT NOT NULL,"
					+ " PAYLOAD_HASH VARCHAR(40) NOT NULL,"
					+ " INSERT_TIME DATE NOT NULL,"
					+ " PRIMARY KEY (TAG_NAME, SINCE) )";
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
				create = connection.prepareStatement(alterIovTagFK);
				failed = create.execute();
				checkAndNotify(failed, alterIovTagFK, prefix);
				create = connection.prepareStatement(alterIovPayloadFK);
				failed = create.execute();
				checkAndNotify(failed, alterIovPayloadFK, prefix);
				create.close();

				for (int i = 0; i < tagList.size(); ++i) {
					PreparedStatement insertTT = connection.prepareStatement("INSERT INTO TAG "
							+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					insertTT.setString(1, tagList.get(i));
					insertTT.setInt(2, 1);
					insertTT.setDate(3, new Date(System.currentTimeMillis()));
					insertTT.setString(4, "This is the tag for " + tagList.get(i) + ".");
					insertTT.setInt(5, 1);
					insertTT.setString(6, "RANDOM");
					insertTT.setInt(7, 111);
					insertTT.setInt(8, 222);
					insertTT.setInt(9, 333);
					insertTT.setInt(10, 444);
					insertTT.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
					failed = insertTT.execute();
					checkAndNotify(failed, "INSERT INTO TAG statement - " + tagList.get(i), prefix);
					insertTT.close();
				}

			} catch (SQLException e) {
				System.out.println(" setupEnvironment() -> SQLException occured. Details: " + e.toString());
			}

			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		/** Environment destroy function for Oracle. */
		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String deleteTagQuery = "DROP TABLE TAG CASCADE CONSTRAINTS";
			String deleteChunkQuery = "DROP TABLE CHUNK CASCADE CONSTRAINTS";
			String deletePayloadQuery = "DROP TABLE PAYLOAD CASCADE CONSTRAINTS";
			String deleteIOVQuery = "DROP TABLE IOV CASCADE CONSTRAINTS";
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
			} catch (SQLException e) {
				System.out.println(" destroyEnvironment() -> SQLException occured. Details: " + e.toString());
			}
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}

	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "OracleDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "Oracle instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// Oracle specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (Oracle default: 1521)");

		/** Help page creation. */
		HelpFormatter formatter = new HelpFormatter();
		if (args.length < 1) {
			System.err.println("Arguments are required for deploying anything...\n");
			formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			return;
		}

		/** Start to parse the command line arguments. */
		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cl = parser.parse(depOps, args);
			HashMap<String, String> optMap = DeployerOptions.mapCommandLine(cl);

			if (optMap.containsKey("HELP")) {
				System.out.println(optMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				OracleEnvironmentDeployer deployer =
						new OracleEnvironmentDeployer(optMap.get("HOST"), optMap.get("PORT"),
								optMap.get("DB"), optMap.get("USER"), optMap.get("PASS"), tagList);
				if (optMap.get("MODE").equals("deploy")) {
					deployer.deployTestEnvironment();
				} else if (optMap.get("MODE").equals("teardown")) {
					deployer.destroyTestEnvironment();
				} else if (optMap.get("MODE").equals("redeploy")) {
					deployer.redeployEnvironment();
				} else {
					System.err.println("Unknown deployment mode: " + optMap.get("MODE"));
					formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
				}
			}

		} catch (ParseException exp) {
			System.err.println("Parsing failed. Details: " + exp.getMessage() + "\n");
			formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
		}

	}

}

