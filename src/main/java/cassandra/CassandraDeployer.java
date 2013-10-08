package cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import utils.TestEnvironmentDeployer;

public class CassandraDeployer {

	private static class CassandraTestEnvironmentDeployer extends TestEnvironmentDeployer {

		private Cluster cassCluster;

		public CassandraTestEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
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
					HFactory.createColumnFamilyDefinition("testKS", "PAYLOAD");
			cassCluster.addColumnFamily(payloadCfDef);
			System.out.println(" setupEnvironment() -> IOV ColumnFamily created...");

			try {
				KeyspaceDefinition ksDef = cassCluster.describeKeyspace("testKS");
				Keyspace testKS = HFactory.createKeyspace(ksDef.getName(), cassCluster);
				
				String tagName = "TEST_TAG";

				Mutator<String> strMutator = HFactory.createMutator(testKS, StringSerializer.get());
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("REVISION", 1));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createColumn("REVISION_TIME", System.currentTimeMillis()));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createColumn("COMMENT", "This is the first and only tag for testing."));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("TIME_TYPE", 1));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("OBJECT_TYPE", "RANDOM"));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("LAST_VALIDATED_TIME", 111));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("END_OF_VALIDITY", 222));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("LAST_SINCE", 333));
				strMutator.addInsertion(tagName, "TAG", HFactory.createColumn("LAST_SINCE_PID", 444));
				strMutator.addInsertion(tagName, "TAG",HFactory.createColumn("CREATION_TIME", System.currentTimeMillis()));
				strMutator.execute();
			} catch (HectorException he) {
				System.out.println("Hector exception occured during initial TAG write:" + he.toString());
			}
			System.out.println(" setupEnvironment() -> Initial TAG added...");
			
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			cassCluster.truncate("testKS", "TAG");
			String response = cassCluster.dropColumnFamily("testKS", "TAG");
			if (response == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop TAG ColumnFamily...");
			}
			cassCluster.truncate("testKS", "IOV");
			response = cassCluster.dropColumnFamily("testKS", "IOV");
			if (response == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop IOV ColumnFamily...");
			}
			cassCluster.truncate("testKS", "PAYLOAD");
			response = cassCluster.dropColumnFamily("testKS", "PAYLOAD");
			if (response == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop Payload ColumnFamily...");
			}
			response = cassCluster.dropKeyspace("testKS");
			if (response == null) {
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
				new CassandraTestEnvironmentDeployer("rsiposc-pc.cern.ch", "9160",
						"TestCondCass", "testUser", "testPass");

		//System.out.println("-------- Cassandra environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- Cassandra environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- Cassandra environment teardown and setup ------------");
		deployer.redeployEnvironment();
	}

}

