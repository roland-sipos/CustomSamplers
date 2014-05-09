package accumulo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

public class AccumuloDeployer {

	private static class AccumuloEnvironmentDeployer extends EnvironmentDeployer {

		private Instance zooKeeper;
		private Connector accumulo;

		public AccumuloEnvironmentDeployer(String host, String port,
				String namespace, String username, String password, String id) {
			super(host, port, namespace, username, password);
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with Accumulo cluster!");
			zooKeeper = new ZooKeeperInstance(getDatabase(), getHost().concat(":").concat(getPort()));
			System.out.println(" initialize() -> ZookeeperInstance created.");
			try {
				accumulo = zooKeeper.getConnector("testUser", "testPass");
				System.out.println(" initialize() -> Whoami? -> " + accumulo.whoami());
				System.out.println(" initialize() -> Connection to Accumulo established.");
			} catch (AccumuloException e) {
				System.out.println(" initialize() -> AccumuloException occured! Details: "
						+ e.toString());
			} catch (AccumuloSecurityException e) {
				System.out.println(" initialize() -> AccumuloSecurityException occured! Details: "
						+ e.toString());
			} catch (Exception e) {
				System.out.println(" initialize() -> Unknown exception occured! Details: "
						+ e.toString());
			}
			
			System.out.println(" initialize() -> Connection established...\n");	
		}

		@Override
		protected void tearDown() {
			System.out.println(" tearDown() -> Starting teardown of resources ...");
			accumulo = null;
			zooKeeper = null;
			System.out.println(" tearDown() -> Teardown finished ...");
		}

		private final Mutation createRow(String rowKey, String colFamily, String colQualifier,
				String visibility, byte[] value) {
			Text rowId = new Text(rowKey);
			Text colF = new Text(colFamily);
			Text colQ = new Text(colQualifier);
			ColumnVisibility colV = new ColumnVisibility(visibility);

			Value v = new Value(value);
			Mutation mutation = new Mutation(rowId);
			// creationTime is always the creation time of the Mutation.
			mutation.put(colF, colQ, colV, System.currentTimeMillis(), v);
			return mutation;
		} 

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");
			try {
				accumulo.tableOperations().create("TAG");
				accumulo.tableOperations().create("IOV");
				accumulo.tableOperations().create("PAYLOAD");

				List<Mutation> muts = new ArrayList<Mutation>();
				muts.add(createRow("TEST_TAG", "META", "REVISION", "public", Bytes.toBytes(111)) );
				muts.add(createRow("TEST_TAG", "META", "REVISION_TIME",
						"public", Bytes.toBytes(System.currentTimeMillis())) );
				muts.add(createRow("TEST_TAG", "META", "COMMENT",
						"public", Bytes.toBytes("This is the first and only tag for testing")) );
				muts.add(createRow("TEST_TAG", "META", "TIME_TYPE", "public", Bytes.toBytes(1)) );
				muts.add(createRow("TEST_TAG", "META", "OBJECT_TYPE", "public", Bytes.toBytes("RANDOM")) );
				muts.add(createRow("TEST_TAG", "META", "LAST_VALIDATED_TIME", "public", Bytes.toBytes(111)) );
				muts.add(createRow("TEST_TAG", "META", "END_OF_VALIDITY", "public", Bytes.toBytes(222)) );
				muts.add(createRow("TEST_TAG", "META", "LAST_SINCE", "public", Bytes.toBytes(333)) );
				muts.add(createRow("TEST_TAG", "META", "LAST_SINCE_PID", "public", Bytes.toBytes(444)) );
				muts.add(createRow("TEST_TAG", "META", "CREATION_TIME",
						"public", Bytes.toBytes(System.currentTimeMillis())) );

				long memBuf = 1000000L; // bytes to store before sending a batch
				long timeout = 1000L; // milliseconds to wait before sending
				int numThreads = 10;

				BatchWriter writer =
						accumulo.createBatchWriter("TAG", memBuf, timeout, numThreads);

				writer.addMutations(muts);
				writer.close();

				System.out.println(" setupEnvironment() -> TAG, IOV, PAYLOAD Tables are created...");
			} catch (AccumuloException e) {
				System.out.println(" setupEnvironment() -> AccumuloException occured! Details: "
						+ e.toString());
			} catch (AccumuloSecurityException e) {
				System.out.println(" setupEnvironment() -> AccumuloSecurityException occured! Details: "
						+ e.toString());
			} catch (TableExistsException e) {
				System.out.println(" setupEnvironment() -> TableExistsException occured! Details: "
						+ e.toString());
			} catch (TableNotFoundException e) {
				System.out.println(" setupEnvironment() -> TableNotFoundException occured! Details: "
						+ e.toString());
			}
			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			try {
				accumulo.tableOperations().delete("TAG");
				accumulo.tableOperations().delete("IOV");
				accumulo.tableOperations().delete("PAYLOAD");
			} catch (AccumuloException e) {
				System.out.println(" destroyEnvironment() -> AccumuloException occured! Details: "
						+ e.toString());
			} catch (AccumuloSecurityException e) {
				System.out.println(" destroyEnvironment() -> AccumuloSecurityException occured! Details: "
						+ e.toString());
			} catch (TableNotFoundException e) {
				System.out.println(" destroyEnvironment() -> TableNotFoundException occured! Details: "
						+ e.toString());
			}
			
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "AccumuloDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "Accumulo clusters. For this, one needs to pass connection details of the cluster.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*AccumuloEnvironmentDeployer deployer =
				new AccumuloEnvironmentDeployer("accumulo1.cern.ch", "2181",
						"testAccumulo", "testUser", "testPass", "3c98b122-4872-4d03-8051-787af3c491f2");*/

		//System.out.println("-------- Accumulo environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- Accumulo environment teardown -----------");
		//deployer.destroyTestEnvironment();
		/*System.out.println("-------- Accumulo environment teardown and setup ------------");
		deployer.redeployEnvironment();*/

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// HBase specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (Accumulo default: 2181)");
		depOps.addOption("i", "id", true, "ID of Accumulo instance");

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

			if (cl.hasOption('i')) {
				optMap.put("ID", cl.getOptionValue('i'));
			} else if (cl.hasOption("id")) {
				optMap.put("ID", cl.getOptionValue("id"));
			} else {
				optMap.put("HELP", "ID argument is missing!");
			}

			if (optMap.containsKey("HELP")) {
				System.out.println(optMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				AccumuloEnvironmentDeployer deployer = new AccumuloEnvironmentDeployer(
						optMap.get("HOST"), optMap.get("PORT"), optMap.get("DB"),
						optMap.get("USER"), optMap.get("PASS"), optMap.get("ID"));
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

