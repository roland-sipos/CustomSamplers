package cassandra;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import utils.TestEnvironmentDeployer;

public class CassandraDeployer {

	private static class CassandraTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Cluster cassCluster;
		
		public CassandraTestEnvironmentDeployer(String host, String port,
				String databaseName, String username,
				String password) {
			super(host, port, databaseName, username, password);
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with Cassandra cluster!");
			cassCluster = HFactory.getOrCreateCluster(super.getDatabase(), super.getHost());
			System.out.println(" initialize() -> Connection established...\n");	
		}

		@Override
		protected void tearDown() {
			try {
				if (cassCluster != null) {
					cassCluster = null;
				}
				System.out.println(" tearDown() -> Connection closed.\n");
			} catch (Exception e){
				System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
			}
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");
			cassCluster.addKeyspace(new ThriftKsDef("testKS"));
			System.out.println(" setupEnvironment() -> testKS Keyspace created...");
			ColumnFamilyDefinition tagCfDef =
					HFactory.createColumnFamilyDefinition("testKS", "TAG");
			cassCluster.addColumnFamily(tagCfDef);
			System.out.println(" setupEnvironment() -> TAG ColumnFamily created...");
			ColumnFamilyDefinition iovCfDef =
					HFactory.createColumnFamilyDefinition("testKS", "IOV");
			cassCluster.addColumnFamily(iovCfDef);
			System.out.println(" setupEnvironment() -> PAYLOAD ColumnFamily created...");
			ColumnFamilyDefinition payloadCfDef =
					HFactory.createColumnFamilyDefinition("testKS", "payload");
			cassCluster.addColumnFamily(payloadCfDef);
			System.out.println(" setupEnvironment() -> IOV ColumnFamily created...");
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			String cfRespTag = cassCluster.dropColumnFamily("testKS", "TAG");
			if (cfRespTag == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop TAG ColumnFamily...");
			}
			String cfRespIov = cassCluster.dropColumnFamily("testKS", "IOV");
			if (cfRespIov == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop IOV ColumnFamily...");
			}
			String cfRespPayload = cassCluster.dropColumnFamily("testKS", "PAYLOAD");
			if (cfRespPayload == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop Payload ColumnFamily...");
			}
			String ksResp = cassCluster.dropKeyspace("testKS");
			if (ksResp == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop TestKS Keyspace...");
			}
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
	}
	
	/**
	    * @param args
		*/
	  public static void main(String[] args) {
	    CassandraTestEnvironmentDeployer deployer =
	            new CassandraTestEnvironmentDeployer("testdb-ora.cern.ch", "1521",
	                    "test", "testUser", "testPass");

	    System.out.println("-------- Cassandra environment setup ------------");
	    deployer.deployTestEnvironment();
	    //System.out.println("------- Cassandra environment teardown -----------");
	    //deployer.destroyTestEnvironment();
	  }
	
	
}
