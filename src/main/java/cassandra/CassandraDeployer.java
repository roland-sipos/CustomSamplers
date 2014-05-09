package cassandra;

import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

public class CassandraDeployer {

	private static class CassandraEnvironmentDeployer extends EnvironmentDeployer {

		private Cluster cassCluster;

		public CassandraEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with Cassandra cluster!");
			cassCluster = HFactory.getOrCreateCluster(super.getDatabase(), super.getHost());
			//cassCluster = Cluster.builder().addContactPoint(super.getHost()).build();
			//cassSession = cassCluster.connect();
			System.out.println(" initialize() -> Connection established...\n");
		}

		@Override
		protected void tearDown() {
			try {
				if (cassCluster != null) {
				}
				System.out.println(" tearDown() -> Connection and Session closed.\n");
			} catch (Exception e) {
				System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
			}
		}

		@Override
		protected void setupEnvironment() {
			System.out.println("-------- Cassandra environment setup ------------");
			System.out.println(" setupEnvironment() -> Setting up the environment...");
			cassCluster.addKeyspace(new ThriftKsDef("testKS"));
			System.out.println(" setupEnvironment() -> testKS Keyspace created...");
			ColumnFamilyDefinition tagCfDef = HFactory
					.createColumnFamilyDefinition("testKS", "TAG");
			cassCluster.addColumnFamily(tagCfDef);
			System.out.println(" setupEnvironment() -> TAG ColumnFamily created...");
			ColumnFamilyDefinition iovCfDef = HFactory
					.createColumnFamilyDefinition("testKS", "IOV");
			cassCluster.addColumnFamily(iovCfDef);
			System.out.println(" setupEnvironment() -> PAYLOAD ColumnFamily created...");
			ColumnFamilyDefinition payloadCfDef = HFactory
					.createColumnFamilyDefinition("testKS", "PAYLOAD");
			cassCluster.addColumnFamily(payloadCfDef);
			System.out.println(" setupEnvironment() -> IOV ColumnFamily created...");

			try {
				KeyspaceDefinition ksDef = cassCluster.describeKeyspace("testKS");
				Keyspace testKS = HFactory.createKeyspace(ksDef.getName(), cassCluster);

				String tagName = "TEST_TAG";

				Mutator<String> strMutator = HFactory.createMutator(testKS, StringSerializer.get());
				//HFactory.createCol
				strMutator.addInsertion(tagName, "TAG", HFactory.createStringColumn("REVISION", "1"));
				strMutator.addInsertion(tagName, "TAG", HFactory.createStringColumn("REVISION_TIME",
								String.valueOf(System.currentTimeMillis())));
				strMutator.addInsertion(tagName, "TAG", HFactory.createStringColumn(
						"COMMENT", "This is the first and only tag for testing."));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createStringColumn("TIME_TYPE", "1"));
				strMutator.addInsertion(tagName, "TAG", 
						HFactory.createStringColumn("OBJECT_TYPE", "RANDOM"));
				strMutator.addInsertion(tagName, "TAG", 
						HFactory.createStringColumn("LAST_VALIDATED_TIME", "111"));
				strMutator.addInsertion(tagName, "TAG", 
						HFactory.createStringColumn("END_OF_VALIDITY", "222"));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createStringColumn("LAST_SINCE", "333"));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createStringColumn("LAST_SINCE_PID", "444"));
				strMutator.addInsertion(tagName, "TAG",
						HFactory.createStringColumn("CREATION_TIME",
								String.valueOf(System.currentTimeMillis())));
				strMutator.execute();
			} catch (HectorException he) {
				System.out.println("Hector exception occured during initial TAG write:" + he.toString());
			}
			System.out.println(" setupEnvironment() -> Initial TAG added...");
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println("------- Cassandra environment teardown -----------");
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			try {
				cassCluster.truncate("testKS", "TAG");
			} catch (Exception e) {
				System.out.println(" destroyEnvironment() -> Failed to truncate TAG CF..."
						+ e.getMessage());
			}
			String response = cassCluster.dropColumnFamily("testKS", "TAG");
			if (response == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop TAG ColumnFamily...");
			}

			try {
				cassCluster.truncate("testKS", "IOV");
			} catch (Exception e) {
				System.out.println(" destroyEnvironment() -> Failed to truncate IOV CF..."
						+ e.getMessage());
			}
			response = cassCluster.dropColumnFamily("testKS", "IOV");
			if (response == null) {
				System.out.println(" destroyEnvironment() -> Failed to drop IOV ColumnFamily...");
			}

			try {
				cassCluster.truncate("testKS", "PAYLOAD");
			} catch (Exception e) {
				System.out.println(" destroyEnvironment() -> Failed to truncate PAYLOAD CF..."
						+ e.getMessage());
			}
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

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "CassandraDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "Cassandra clusters. For this, one needs to pass connection details of the cluster.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// HBase specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (Cassandra default: 9160)");

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
				CassandraEnvironmentDeployer deployer = new CassandraEnvironmentDeployer(
						optMap.get("HOST"), optMap.get("PORT"), optMap.get("DB"),
						optMap.get("USER"), optMap.get("PASS"));
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
