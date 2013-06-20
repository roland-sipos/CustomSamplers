package riak;

import java.util.Set;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;

import utils.TestEnvironmentDeployer;

public class RiakDeployer {

	private static class RiakTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private IRiakClient riakClient = null;
		private int totalMaxConnections = 0;
		
		public RiakTestEnvironmentDeployer(String host, String port,
				String databaseName, String entityName, String username, 
				String password, int totalMaxConnections) {
			super(host, port, databaseName, entityName, username, password);
		}

		@Override
		protected void initialize() {
			HTTPClientConfig.Builder configBuilder = new HTTPClientConfig.Builder();
			String[] hosts = getHost().split(":");
			configBuilder.withHost(hosts[0]);
			configBuilder.withPort(Integer.parseInt(getPort()));
			System.out.println(" initialize() -> HTTPClientConfig.Builder is ready...");
			
			HTTPClientConfig clientConfig = configBuilder.build();
			System.out.println(" initialize() -> HTTPClientConfig build successfull!");
			
			HTTPClusterConfig clusterConf = new HTTPClusterConfig(totalMaxConnections);
			clusterConf.addClient(clientConfig);
			for (int i = 0; i < hosts.length; ++i) {
				System.out.println(" initialize() -> Adding host to HTTPClusterConfig: " + hosts[i]);
				clusterConf.addHosts(hosts[i]);
			}

			try {
				riakClient = RiakFactory.newClient(clusterConf);
				riakClient.ping();
				System.out.println(" initialize() -> RIAK connection prepared!");
			} catch (RiakException e) {
				System.out.println(" initialize() -> RIAKException occured on connection attempt." 
						+ " Details: " + e.toString());
			}
		}

		@Override
		protected void tearDown() {
			try {
	        	if (riakClient != null)
	        		riakClient.shutdown();
	        	System.out.println(" tearDown() -> Connection closed.\n");
	        }
	        catch (Exception e){
	        	System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
	        }
		}

		@Override
		protected void setupEnvironment() {
			try {
				Set<String> buckets = riakClient.listBuckets();
				System.out.println(" setupEnvironment() -> Found buckets: " + buckets.toString());
				if (buckets.contains(getEntity())) {
					System.out.println(" setupEnvironment() -> " + getEntity() + " bucket already available.");
					return;
				}
				Bucket bucket = riakClient.createBucket(getEntity())
						.allowSiblings(false).nVal(1).execute();
				System.out.println(" setupEnvironment() -> " + bucket.getName() + " bucket created.");
			} catch (RiakException e) {
				System.out.println(" setupEnvironment() -> RiakException occured on bucket creation." 
						+ " Details: " + e.toString());
			}
		}

		@Override
		protected void destroyEnvironment() {
			Bucket bucket = null;
			try {
				bucket = riakClient.fetchBucket(getEntity()).execute();
				for (String key : bucket.keys()) {
					bucket.delete(key).execute();
				}
				System.out.println(" destroyEnvironment() -> Deleting content of " + getEntity() + " bucket.");
			} catch (RiakException e) {
				System.out.println(" destroyEnvironment() -> RiakException occured while deleting element " 
						+ " of " + bucket.getName() + "!");
			}
			
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RiakTestEnvironmentDeployer deployer =
				new RiakTestEnvironmentDeployer(
						"188.184.20.73:188.184.20.74:137.138.241.22:137.138.241.69",
						"8098", "", "binaries", "", "", 10);
	
		System.out.println("-------- RIAK environment setup ------------");
		deployer.deployTestEnvironment();
		//System.out.println("------- RIAK environment teardown -----------");
		//deployer.destroyTestEnvironment();
		
	}

}
