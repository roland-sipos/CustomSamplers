package couchdb;

import java.net.MalformedURLException;
import java.util.List;

import org.ektorp.CouchDbConnector;
import org.ektorp.Revision;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import utils.EnvironmentDeployer;

public class CouchDeployer {

	private static class CouchTestEnvironmentDeployer extends EnvironmentDeployer {

		StdCouchDbInstance dbInstance = null;
		CouchDbConnector couch = null;
		
		public CouchTestEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize -> Initializing database connection...");
			
			try {
				HttpClient authHttpClient = new StdHttpClient.Builder()
						.url(getHost().concat(":").concat(getPort()))
						.connectionTimeout(50000)
						.build();
				dbInstance = new StdCouchDbInstance(authHttpClient);
				couch = dbInstance.createConnector(getDatabase(), false);
			} catch (MalformedURLException e) {
				System.out.println(" initialize -> MalformedURLException occured... " + e.getMessage());
				e.printStackTrace();
			}
			System.out.println(" initialize -> Initialization successfull.");
		}

		@Override
		protected void tearDown() {
			System.out.println(" tearDown -> Cleaning up connection...");
			couch = null;
			System.out.println(" tearDown -> Connection cleaned up.");
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Found database: " + couch.getDatabaseName());
			couch = dbInstance.createConnector(getDatabase(), true);
			/*System.out.println(" setupEnvironment() -> Adding initial TAG document...");
			Map<String, Object> tDoc = new HashMap<String, Object>();
			tDoc.put("_id", "TEST_TAG");
			tDoc.put("revision", "1");
			tDoc.put("revision_time", String.valueOf(System.currentTimeMillis()));
			tDoc.put("comment", "This is the first and only tag for testing.");
			tDoc.put("time_type", "1");
			tDoc.put("object_type", "RANDOM");
			tDoc.put("last_validated_time", "111");
			tDoc.put("end_of_validity", "222");
			tDoc.put("last_since", "333");
			tDoc.put("last_since_pid", "444");
			tDoc.put("creation_time", String.valueOf(System.currentTimeMillis()));
			couch.create(tDoc);*/
			System.out.println(" setupEnvironment() -> Environment set up successfull.");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			List<String> ids = couch.getAllDocIds();
			for (int i = 0; i < ids.size(); ++i) {
				String id = ids.get(i);
				List<Revision> revs = couch.getRevisions(id);
				for (int j = 0; j < revs.size(); ++j) {
					Revision r = revs.get(j);
					couch.delete(id, r.getRev());
				}
			}
			couch.compact();
			dbInstance.deleteDatabase(super.getDatabase());
			System.out.println(" destroyEnvironment() -> Environment destroyed.");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CouchTestEnvironmentDeployer deployer =
				new CouchTestEnvironmentDeployer(
						"http://test-couchdb.cern.ch", "5984", "test" , "testUser", "testPass");

		System.out.println("-------- CouchDB environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- CouchDB environment teardown -----------");
		//deployer.destroyTestEnvironment();
		//System.out.println("-------- CouchDB environment teardown and setup ------------");
		//deployer.redeployEnvironment();

	}

}
