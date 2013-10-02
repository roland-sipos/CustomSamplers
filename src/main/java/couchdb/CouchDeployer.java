package couchdb;

import org.jcouchdb.db.Database;
import org.jcouchdb.db.Server;
import org.jcouchdb.document.BaseDocument;

import utils.TestEnvironmentDeployer;

public class CouchDeployer {

	private static class CouchTestEnvironmentDeployer extends TestEnvironmentDeployer {

		Database couch = null;
		
		public CouchTestEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize -> Initializing database connection...");
			couch = new Database(getHost(), Integer.parseInt(getPort()), getDatabase());
			Server server = couch.getServer();
			if (!server.listDatabases().contains(super.getDatabase()));
			{
				System.out.println(" initialize -> Requested DB doesn't exist, creating it...");
				server.createDatabase(super.getDatabase());
			}
			System.out.println(" initialize -> Initialization successfull.");
		}

		@Override
		protected void tearDown() {
			System.out.println(" tearDown -> Cleaning up connection...");
			couch = null;
			System.out.println(" initialize -> Connection cleaned up.");
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Found database: " + couch.getName());
			System.out.println(" setupEnvironment() -> Adding initial TAG document...");
			BaseDocument tDoc = new BaseDocument();
			tDoc.setId("TEST_TAG");
			tDoc.setProperty("revision", "1");
			tDoc.setProperty("revision_time", String.valueOf(System.currentTimeMillis()));
			tDoc.setProperty("comment", "This is the first and only tag for testing.");
			tDoc.setProperty("time_type", "1");
			tDoc.setProperty("object_type", "RANDOM");
			tDoc.setProperty("last_validated_time", "111");
			tDoc.setProperty("end_of_validity", "222");
			tDoc.setProperty("last_since", "333");
			tDoc.setProperty("last_since_pid", "444");
			tDoc.setProperty("creation_time", String.valueOf(System.currentTimeMillis()));
			couch.createOrUpdateDocument(tDoc);
			System.out.println(" setupEnvironment() -> Environment set up successfull.");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			Server server = couch.getServer();
			server.deleteDatabase(couch.getName());
			server.createDatabase(super.getDatabase());
			System.out.println(" destroyEnvironment() -> Environment destroyed.");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CouchTestEnvironmentDeployer deployer =
				new CouchTestEnvironmentDeployer(
						"testdb-pc.cern.ch", "5984", "test" , "testUser", "testPass");

		//System.out.println("-------- CouchDB environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- CouchDB environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- CouchDB environment teardown and setup ------------");
		deployer.redeployEnvironment();

	}

}
