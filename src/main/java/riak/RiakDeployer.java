package riak;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

import utils.EnvironmentDeployer;

public class RiakDeployer {

	private static class RiakTestEnvironmentDeployer extends EnvironmentDeployer {

		private IRiakClient riakClient = null;
		private int totalMaxConnections = 0;
		private List<String> bucketNames = null;
		private String protocol = null;

		public RiakTestEnvironmentDeployer(String host, String port, String databaseName,
				String username, String password,
				int totalMaxConnections, String bucketNames, String protocol) {
			super(host, port, databaseName, username, password);
			this.totalMaxConnections = totalMaxConnections;
			this.bucketNames = Arrays.asList(bucketNames.split("\\s+"));
			System.out.println(this.bucketNames.toString());
			this.protocol = protocol;
		}
		
		@Override
		protected void initialize() {
			String[] hosts = getHost().split(":");
			if (protocol.equals("HTTP")) {
				HTTPClientConfig.Builder configBuilder = new HTTPClientConfig.Builder();
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

			} else if (protocol.equals("RAW-PB")) {
				PBClientConfig.Builder configBuilder = 
						PBClientConfig.Builder.from(PBClientConfig.defaults());
				configBuilder.withHost(hosts[0]);
				configBuilder.withPort(Integer.parseInt(getPort()));
				System.out.println(" initialize() -> PBClientConfig.Builder is ready...");
				configBuilder.build();
				System.out.println(" initialize() -> PBClientConfig build successfull!");
				PBClientConfig clientConfig = configBuilder.build();
				PBClusterConfig clusterConf = new PBClusterConfig(totalMaxConnections);
				clusterConf.addClient(clientConfig);
				for (int i = 0; i < hosts.length; ++i) {
					System.out.println(" initialize() -> Adding host to PBClusterConfig: " + hosts[i]);
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
				
			} else {
				System.err.println("Cannot make IRiakClient... Protocol is unknown: "
						+ protocol + " (possible options: HTTP or RAW-PB");
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

		@SuppressWarnings("deprecation")
		@Override
		protected void setupEnvironment() {
			try {
				Set<String> buckets = riakClient.listBuckets();
				System.out.println(" setupEnvironment() -> Found buckets: " + buckets.toString());
				
				Iterator<String> i = bucketNames.iterator();
				while (i.hasNext()) {
					String bN = i.next();
					Bucket b = riakClient.fetchBucket(bN).execute();
					riakClient.updateBucket(b)
						.allowSiblings(false).nVal(1)
						.w(Quora.QUORUM).dw(Quora.QUORUM).pw(0)
						.r(Quora.QUORUM).rw(Quora.QUORUM).pr(0)
						.execute();
					b.store(bN, "Init store").execute();
					System.out.println(" setupEnvironment() -> " + bN + " bucket created ...");
				}
				System.out.println(" setupEnvironment() -> Found buckets after creation: "
						+ riakClient.listBuckets().toString());
				
				Bucket b = riakClient.fetchBucket("TAG").execute();
				b.store("TEST_TAG", "This is the initial TAG for testing.").execute();
				IRiakObject tTag = b.fetch("TEST_TAG").execute();
				System.out.println("Stored initial TAG: Key:"
						+ tTag.getKey() + " Value:" + tTag.getValueAsString());

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

		@SuppressWarnings("deprecation")
		@Override
		protected void destroyEnvironment() {
			try {
				Iterator<String> i = bucketNames.iterator();
				while (i.hasNext()) {
					String bN = i.next();
					Bucket b = riakClient.fetchBucket(bN).execute();
					truncateBucket(b);
					System.out.println(" destroyEnvironment() -> Deleted content of " + bN + " bucket.");
				}
				System.out.println(" destroyEnvironment() -> Found buckets after truncate: "
						+ riakClient.listBuckets().toString());
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
						"8087", "" , "", "", 10, "TAG IOV PAYLOAD CHUNK", "RAW-PB"); // RAW-PB: 8087

		//System.out.println("-------- RIAK environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- RIAK environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- RIAK environment teardown and setup ------------");
		deployer.redeployEnvironment();

	}

}
