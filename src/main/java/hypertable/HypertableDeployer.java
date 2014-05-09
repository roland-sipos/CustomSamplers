package hypertable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import org.hypertable.thrift.*;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

public class HypertableDeployer {

	private static class HypertableEnvironmentDeployer extends EnvironmentDeployer {

		private static ThriftClient hyperTClient;
		private static Long hyperTNS = -1L;

		public HypertableEnvironmentDeployer(String host, String port,
				String namespace, String username, String password) {
			super(host, port, namespace, username, password);
		}

		private String readFile(final InputStream inputStream) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			StringBuilder stringBuilder = new StringBuilder();
			String ls = System.getProperty("line.separator");
			try {
				while( ( line = reader.readLine() ) != null ) {
					stringBuilder.append( line );
					stringBuilder.append( ls );
				}
			} catch (IOException e) {
				System.out.println("IOException occured: " + e.toString());
			}
			return stringBuilder.toString();
		}

		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with Hypertable cluster!");
			try {
				hyperTClient = ThriftClient.create(getHost(), Integer.valueOf(getPort()));
				if (!hyperTClient.namespace_exists(getDatabase())) {
					hyperTClient.namespace_create(getDatabase());
					System.out.println(" initialize() -> Namespace " + getDatabase() + " created.");
				} else {
					System.out.println(" initialize() -> Namespace " + getDatabase() + " already exists.");
				}
				hyperTNS = hyperTClient.namespace_open(getDatabase());
				System.out.println(" initialize() -> Namespace " + getDatabase() + " opened.");
			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured. Details: " + e.toString());
			} catch (TTransportException e) {
				System.out.println("TTransportException occured. Details: " + e.toString());
			} catch (TException e) {
				System.out.println("TException occured. Details: " + e.toString());
			} catch (ClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(" initialize() -> Connection established...\n");	
		}

		@Override
		protected void tearDown() {
			System.out.println(" tearDown() -> Starting teardown of resources ...");
			try {
				if (hyperTClient != null && hyperTNS != -1)
					hyperTClient.namespace_close(hyperTNS);
			}
			catch (Exception ce) {
				System.err.println(" tearDown() -> Problem closing namespace "
						+ getDatabase()
						+ " Details: " + ce.getMessage());
			}
			System.out.println(" tearDown() -> Teardown finished ...");
		}

		private final Cell createCell(String rowKey, String columnFamily,
				String qualifier, byte[] value) {
			Key key = new Key();
			key.setRow(rowKey).setColumn_family(columnFamily).setColumn_qualifier(qualifier);
			Cell cell = new Cell();
			cell.setKey(key);
			cell.setValue(value);
			return cell;
		}

		@Override
		protected void setupEnvironment() {
			System.out.println(" setupEnvironment() -> Setting up the environment...");

			try {
				if (hyperTClient.table_exists(hyperTNS, "TAG") 
						|| hyperTClient.table_exists(hyperTNS, "IOV") 
						|| hyperTClient.table_exists(hyperTNS, "PAYLOAD")) {
					System.out.println(" initialize() -> Namespace testNS is not clean! "
						+ "Some of the following Tables are exists: TAG, IOV, PAYLOAD");
				} else {

					String tagSchemaStr = readFile(Thread.currentThread()
							.getContextClassLoader()
							.getResourceAsStream("hypertable/Schema_TAG.xml"));
					String iovSchemaStr = readFile(Thread.currentThread()
							.getContextClassLoader()
							.getResourceAsStream("hypertable/Schema_IOV.xml"));
					String payloadSchemaStr = readFile(Thread.currentThread()
							.getContextClassLoader()
							.getResourceAsStream("hypertable/Schema_PAYLOAD.xml"));
					
					hyperTClient.table_create(hyperTNS, "TAG", tagSchemaStr);
					hyperTClient.table_create(hyperTNS, "IOV", iovSchemaStr);
					hyperTClient.table_create(hyperTNS, "PAYLOAD", payloadSchemaStr);
					System.out.println(" setupEnvironment() -> Tables are created.");
					
					long mutator = hyperTClient.mutator_open(hyperTNS, "TAG", 0, 0);
					Key tagKey = new Key();
					tagKey.setRow("TEST_TAG").setColumn_family("META");
					List<Cell> cells = new ArrayList<Cell>();
					
					cells.add(createCell("TEST_TAG", "META", "REVISION",
							Bytes.toBytes(111)));
					cells.add(createCell("TEST_TAG", "META", "REVISION_TIME",
							Bytes.toBytes(System.currentTimeMillis()) ));
					cells.add(createCell("TEST_TAG", "META", "COMMENT",
							"This is the first and only tag for testing".getBytes() ));
					cells.add(createCell("TEST_TAG", "META", "TIME_TYPE", 
							Bytes.toBytes(1) ));
					cells.add(createCell("TEST_TAG", "META", "OBJECT_TYPE", 
							"RANDOM".getBytes() ));
					cells.add(createCell("TEST_TAG", "META", "LAST_VALIDATED_TIME", 
							Bytes.toBytes(111) ));
					cells.add(createCell("TEST_TAG", "META", "END_OF_VALIDITY", 
							Bytes.toBytes(222) ));
					cells.add(createCell("TEST_TAG", "META", "LAST_SINCE", 
							Bytes.toBytes(333) ));
					cells.add(createCell("TEST_TAG", "META", "LAST_SINCE_PID", 
							Bytes.toBytes(444) ));
					cells.add(createCell("TEST_TAG", "META", "CREATION_TIME", 
							Bytes.toBytes(System.currentTimeMillis()) ));

					hyperTClient.mutator_set_cells(mutator, cells);
					hyperTClient.mutator_close(mutator);
				}
			} catch (ClientException ce) {
				System.out.println("ClientException occured. Details: " + ce.toString());
			} catch (TException te) {
				System.out.println("TException occured. Details: " + te.toString());
			}

			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			try {
				hyperTClient.table_drop(hyperTNS, "TAG", Boolean.TRUE); // True: If exists.
				hyperTClient.table_drop(hyperTNS, "IOV", Boolean.TRUE);
				hyperTClient.table_drop(hyperTNS, "PAYLOAD", Boolean.TRUE);
				System.out.println(" destroyEnvironment() -> Tables are (if existed,) dropped.");
			} catch (ClientException ce) {
				System.out.println("ClientException occured. Details: " + ce.toString());
			} catch (TException te) {
				System.out.println("TException occured. Details: " + te.toString());
			}
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "HypertableDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "HyperTable clusters. For this, one needs to pass connection details of the cluster.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*HypertableEnvironmentDeployer deployer =
				new HypertableEnvironmentDeployer("hypertable1.cern.ch", "38080",
						"testNamespace", "testUser", "testPass");*/

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// HBase specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (Hypertable default: 38080)");

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
				HypertableEnvironmentDeployer deployer = new HypertableEnvironmentDeployer(
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

