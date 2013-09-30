package riak;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.basho.riak.client.DefaultRiakObject;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.LinkWalkStep.Accumulate;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

import utils.TestEnvironmentDeployer;

public class RiakDeployer {

	private static class RiakTestEnvironmentDeployer extends TestEnvironmentDeployer {

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

		protected IRiakClient getRiakClient() {
			return riakClient;
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

		
		/*deployer.initialize();
		
		try {
			
			deployer.truncateBucket(deployer.getRiakClient().fetchBucket("test").execute());
			deployer.truncateBucket(deployer.getRiakClient().fetchBucket("test2").execute());
			
			Bucket b = deployer.getRiakClient().fetchBucket("test2").execute();
			StoreObject<IRiakObject> sObj = b.store("kortefa_key", "ittakorted");
			IRiakObject res2 = sObj.returnBody(true).execute();
			res2.addUsermeta("ID", String.valueOf(1));
			
			b = deployer.getRiakClient().fetchBucket("test").execute();
			//StoreObject<IRiakObject> sObj = b.store("almafa", "kortefa");
			IRiakObject res = b.store("almafa", "kortefa").returnBody(true).execute();
			//res.addLink(new RiakLink("test2", "kortefa_key", "linkName"));
			res.addUsermeta("ID", String.valueOf(1));
			
			b = deployer.getRiakClient().fetchBucket("test").execute();
			Bucket b2 = deployer.getRiakClient().fetchBucket("test2").execute();
			IRiakObject lo = b.fetch("almafa").execute();
			lo.addLink(new RiakLink("test2", "kortefa_key", "linkName"));
			b.store(lo).execute();
			
			IRiakObject o = b.fetch("almafa").execute();
			System.out.println(o.getKey() + " - " + o.getValueAsString());
			System.out.println(o.hasLinks());
			System.out.println(o.getLinks().toString());
			IRiakObject o2 = b2.fetch("kortefa_key").execute();
			System.out.println(o2.getKey() + " - " + o2.getValueAsString());
			System.out.println(o2.hasLinks());
			System.out.println(o2.getLinks().toString());
			
			System.out.println(deployer.getRiakClient().listBuckets());

			
			
			PBClientConfig.Builder configBuilder = 
					PBClientConfig.Builder.from(PBClientConfig.defaults());
			configBuilder.withHost("veszterdb1.cern.ch");
			configBuilder.withPort(8087);
			configBuilder.build();

			PBClientConfig clientConfig = configBuilder.build();
			PBClusterConfig clusterConf = new PBClusterConfig(50);
			clusterConf.addClient(clientConfig);
				
			clusterConf.addHosts("veszterdb2.cern.ch");
			clusterConf.addHosts("veszterdb3.cern.ch");
			clusterConf.addHosts("veszterdb4.cern.ch");
			clusterConf.addHosts("veszterdb5.cern.ch");
			clusterConf.addHosts("veszterdb6.cern.ch");
			
			IRiakClient pbClient = RiakFactory.newClient(clusterConf);
			
			//WalkResult wr = deployer.getRiakClient().walk(o).addStep("test2", "linkName").execute();
			WalkResult wr = pbClient.walk(o).addStep("test2", "linkName").execute();
			System.out.println(wr.toString());
			
			//WalkResult res = deployer.getRiakClient().walk(dro).addStep("test2", "linkName", true).execute();
			Iterator<Collection<IRiakObject> > i = wr.iterator();
			int count = 0;
			IRiakObject plObj = null;
			while (i.hasNext())
			{
				System.out.println("has next!!!");
				count++;
				Collection<IRiakObject> c = i.next();
				for (IRiakObject ito : c)
				{
					System.out.println(count + ". link shows to PAYLOAD:" + ito.getKey());
					plObj = ito;
					System.out.println(ito.getValueAsString());
				}
			}
			
			b.delete("almafa").execute();
			
		} catch (RiakRetryFailedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RiakException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		deployer.tearDown();*/

	}

}
