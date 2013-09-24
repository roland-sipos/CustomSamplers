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

		public RiakTestEnvironmentDeployer(String host, String port, String databaseName,
				String username, String password, int totalMaxConnections) {
			super(host, port, databaseName, username, password);
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
				Bucket b = null;
				if (buckets.contains("TAG")) {
					System.out.println(" setupEnvironment() -> TAG bucket already available ...");
				} else {
					b = riakClient.createBucket("TAG")
						.allowSiblings(false).nVal(1).lastWriteWins(true).execute();
					b.store("TAG", "Init store").execute();
					System.out.println(" setupEnvironment() -> TAG bucket created ...");
				}
				if (buckets.contains("PAYLOAD")) {
					System.out.println(" setupEnvironment() -> PAYLOAD bucket already available ...");
				} else {
					b = riakClient.createBucket("PAYLOAD")
						.allowSiblings(false).nVal(1).lastWriteWins(true).execute();
					b.store("PAYLOAD", "Init store").execute();
					System.out.println(" setupEnvironment() -> PAYLOAD bucket created ...");
				}
				if (buckets.contains("CHUNK")) {
					System.out.println(" setupEnvironment() -> CHUNK bucket already available ...");
				} else {
					b = riakClient.createBucket("CHUNK")
						.allowSiblings(false).nVal(1).lastWriteWins(true).execute();
					b.store("CHUNK", "Init store").execute();
					System.out.println(" setupEnvironment() -> CHUNK bucket created ...");
				}
				System.out.println(riakClient.listBuckets().toString());
			} catch (RiakException e) {
				System.out.println(" setupEnvironment() -> RiakException occured on bucket creation." 
						+ " Details: " + e.toString());
			}
		}

		private void truncateBucket(Bucket bucket) throws RiakException {
			for (String key : bucket.keys()) {
				bucket.delete(key).execute();
			}
		}

		@Override
		protected void destroyEnvironment() {
			Bucket bucket = null;
			try {
				bucket = riakClient.fetchBucket("TAG").execute();
				truncateBucket(bucket);
				System.out.println(" destroyEnvironment() -> Deleted content of TAG bucket.");
				bucket = riakClient.fetchBucket("PAYLOAD").execute();
				truncateBucket(bucket);
				System.out.println(" destroyEnvironment() -> Deleted content of PAYLOAD bucket.");
				bucket = riakClient.fetchBucket("CHUNK").execute();
				truncateBucket(bucket);
				System.out.println(" destroyEnvironment() -> Deleted content of CHUNK bucket.");
			} catch (RiakException e) {
				System.out.println(" destroyEnvironment() -> RiakException occured while deleting buckets!");
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RiakTestEnvironmentDeployer deployer =
				new RiakTestEnvironmentDeployer(
						"veszterdb1.cern.ch:veszterdb2.cern.ch:veszterdb3.cern.ch:"+
						"veszterdb4.cern.ch:veszterdb5.cern.ch:veszterdb6.cern.ch",
						"8098", "" , "", "", 10);

		//System.out.println("-------- RIAK environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- RIAK environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- RIAK environment teardown and setup ------------");
		deployer.redeployEnvironment();

	}

}
