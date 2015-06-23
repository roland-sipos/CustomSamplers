package cassandra;

import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

public class CassandraDeployer {

	private static class CassandraEnvironmentDeployer extends EnvironmentDeployer {

		private Cluster cassCluster;
		private Session cassSession;

		private boolean chunkSupport;
		private String compactionStrategy;

		public CassandraEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password, String chunk, String compact) {
			super(host, port, databaseName, username, password);
			chunkSupport = Boolean.valueOf(chunk);
			compactionStrategy = compact;
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with Cassandra cluster!");
			Builder clusterBuilder = Cluster.builder();
			if (getHost().contains(",")) {
				String hosts[] = getHost().split(",");
				for (int i = 0; i < hosts.length; ++i) {
					clusterBuilder.addContactPoint(hosts[i]);
				}
			} else {
				clusterBuilder.addContactPoint(getHost());
			}
			cassCluster = clusterBuilder.withPort(Integer.valueOf(getPort())).build();
			cassSession = cassCluster.connect();
			System.out.println(" initialize() -> Connection established...\n");
		}

		@Override
		protected void tearDown() {
			try {
				if (cassCluster != null) {
					cassSession.close();
					cassCluster.close();
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
			
			cassSession.execute("CREATE KEYSPACE conddb WITH replication "
					+ "= {'class':'SimpleStrategy', 'replication_factor':1};");
			System.out.println(" setupEnvironment() -> conddb Keyspace created...");

			String iovTableCqlsh = "CREATE TABLE conddb.iov ("
					+ "tag text, "
					+ "since bigint, ";
			if (chunkSupport) {
				iovTableCqlsh += "pl_hash text, hash list<text>, ";
			} else {
				iovTableCqlsh += "hash text, ";
			}
			iovTableCqlsh += "PRIMARY KEY (tag, since) );";

			System.out.println(" setupEnvironment() -> Creating IOV ColumnFamily with: " + iovTableCqlsh);
			cassSession.execute(iovTableCqlsh);	
			System.out.println(" setupEnvironment() -> iov ColumnFamily created...");

			String payloadTableCqlsh = "CREATE TABLE conddb.payload (";
			if (chunkSupport) {
				payloadTableCqlsh += "pl_hash text, hash text, data blob, ";
				payloadTableCqlsh += "PRIMARY KEY ((pl_hash), hash) );";
			} else {
				payloadTableCqlsh += "hash text, data blob, ";
				payloadTableCqlsh += "PRIMARY KEY (hash) ) ";
			}
			payloadTableCqlsh += " WITH caching='KEYS_ONLY' AND";
			if (compactionStrategy.equals("LEVELED")) {
				payloadTableCqlsh += " compaction={'class': 'LeveledCompactionStrategy'};";
			} else if (compactionStrategy.equals("SIZE-TIERED")) {
				payloadTableCqlsh += " compaction={'class': 'SizeTieredCompactionStrategy'};";
			}

			System.out.println(" setupEnvironment() -> Creating Payload ColumnFamily with: " 
					+ payloadTableCqlsh);
			cassSession.execute(payloadTableCqlsh);
			System.out.println(" setupEnvironment() -> payload ColumnFamily created...");
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println("------- Cassandra environment teardown -----------");
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			cassSession.execute("DROP TABLE conddb.payload;");
			System.out.println(" destroyEnvironment() -> payload ColumnFamily dropped...");
			cassSession.execute("DROP TABLE conddb.iov;");
			System.out.println(" destroyEnvironment() -> iov ColumnFamily dropped...");
			cassSession.execute("DROP KEYSPACE conddb;");
			System.out.println(" destroyEnvironment() -> conddb Keyspace dropped...");
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "CassandraThriftDeployer [OPTIONS]";
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
		depOps.addOption("p", "port", true, "port of the host (Native transport port: 9042)");
		depOps.addOption("c", "chunks", true, "deploy with chunk schema (true or false)");
		depOps.addOption("o", "compaction", true, "compaction strategy (LEVELED or SIZE-TIERED");

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

			// Cassandra specific options are parsed manually here:
			if (cl.hasOption('c')) {
				optMap.put("CHUNK", cl.getOptionValue('c'));
			} else if (cl.hasOption("chunks")) {
				optMap.put("CHUNK", cl.getOptionValue("chunk"));
			} else {
				optMap.put("HELP", "Chunk argument is missing!");
			}
			if (cl.hasOption('o')) {
				optMap.put("COMPACT", cl.getOptionValue('o'));
			} else if (cl.hasOption("compaction")) {
				optMap.put("COMPACT", cl.getOptionValue("compaction"));
			} else {
				optMap.put("HELP", "Compaction argument is missing!");
			}

			if (optMap.containsKey("HELP")) {
				System.out.println(optMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				CassandraEnvironmentDeployer deployer = new CassandraEnvironmentDeployer(
						optMap.get("HOST"), optMap.get("PORT"), optMap.get("DB"),
						optMap.get("USER"), optMap.get("PASS"), optMap.get("CHUNK"),
						optMap.get("COMPACT"));
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
