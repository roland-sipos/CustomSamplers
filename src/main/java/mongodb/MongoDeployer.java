package mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import utils.TestEnvironmentDeployer;

public class MongoDeployer {

	private static class MongoTestEnvironmentDeployer extends TestEnvironmentDeployer {

		DB mongoDB = null;

		public MongoTestEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected void initialize() {
			System.out.println("initialize -> Initialization started...");
			//MongoClientOptions.Builder mongoConf = new Builder();
			//mongoConf.socketTimeout(50000);
			MongoClient mongoClient = null;
			try {
				ServerAddress address = new ServerAddress(getHost(), Integer.parseInt(getPort()));
				//mongoClient = new MongoClient(address, mongoConf.build());
				mongoClient = new MongoClient(address);
			} catch (Exception e) {
				System.out.println("initialize -> Exception occured: " + e.toString());
			}

			mongoDB = mongoClient.getDB(getDatabase());
			boolean auth = mongoDB.isAuthenticated();
			if (!auth) {
				System.out.println("initialize -> Needs authentication...");
				if (!getUsername().equals("") && !getPassword().equals("")) {
					mongoDB.authenticate(getUsername(), getPassword().toCharArray());
				} else {
					System.out.println("initialize -> Username or passwords is empty!");
				}
			} else {
				System.out.println(" initialize -> Doesn't need to authenticate...");
			}
			System.out.println("initialize -> Initialization successfull!");
		}

		@Override
		protected void tearDown() {
			System.out.println(" tearDown -> Teardown started...");
			mongoDB = null;
			System.out.println(" tearDown -> Teardown successfull!");
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment -> Enviroment setup started...");
			DBObject options = BasicDBObjectBuilder.start()
					.add("capped", true)
					.add("size", 5242880)
					.add("max", 1000).get();
			mongoDB.createCollection("TAG", options);
			mongoDB.createCollection("IOV", options);
			mongoDB.createCollection("PAYLOAD", options);
			System.out.println(" setupEnvironment -> Environment setup successfull!");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" setupEnvironment -> Enviroment destroy started...");
			mongoDB.getCollection("TAG").drop();
			mongoDB.getCollection("IOV").drop();
			mongoDB.getCollection("PAYLOAD").drop();
			mongoDB.getCollection("PAYLOAD.files").drop();
			mongoDB.getCollection("PAYLOAD.chunks").drop();
			System.out.println(" setupEnvironment -> Enviroment successfully destroyed!");
		}
		
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoTestEnvironmentDeployer deployer =
				new MongoTestEnvironmentDeployer(
						"mongo-node1.cern.ch", "27017", "test" , "testUser", "testPass");

		System.out.println("-------- MongoDB environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- MongoDB environment teardown -----------");
		//deployer.destroyTestEnvironment();
		//System.out.println("-------- MongoDB environment teardown and setup ------------");
		//deployer.redeployEnvironment();

	}
	
}
