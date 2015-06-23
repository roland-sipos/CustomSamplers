package hbase;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

public class HBaseDeployer {

	private static class HBaseEnvironmentDeployer extends EnvironmentDeployer {

		private static Configuration hbaseConf;
		private static HBaseAdmin hbaseAdmin;

		public HBaseEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
		}

		@SuppressWarnings({ "static-access", "deprecation" })
		@Override
		protected void initialize() {
			System.out.println(" initialize() -> Initializing connection with HBase cluster!");
			
			hbaseConf = new HBaseConfiguration().create();
			hbaseConf.set("hbase.zookeeper.quorum", super.getHost());
			hbaseConf.set("hbase.zookeeper.property.clientPort", super.getPort());
			hbaseConf.set("hbase.master", super.getHost().concat(":60000"));

			try {
				HBaseAdmin.checkHBaseAvailable(hbaseConf);
				hbaseAdmin = new HBaseAdmin(hbaseConf);
			} catch (MasterNotRunningException e) {
				System.out.println("MasterNotRunningException occured. Details: " + e.toString());
			} catch (ZooKeeperConnectionException e) {
				System.out.println("ZooKeeperConnectionException occured. Details: " + e.toString());
			}
			System.out.println(" initialize() -> Connection established...\n");	
		}

		@Override
		protected void tearDown() {
			try {
				if (hbaseConf != null) {
					hbaseConf = null;
				}
				if (hbaseAdmin != null) {
					hbaseAdmin.close();
				}
				System.out.println(" tearDown() -> Connection closed.\n");
			} catch (Exception e){
				System.out.println(" tearDown() -> Connection closing failed: " + e.toString());
			}
		}

		@SuppressWarnings("resource")
		@Override
		protected void setupEnvironment() {
			System.out.println("----------- HBase environment setup ----------");
			System.out.println(" setupEnvironment() -> Setting up the environment...");

			try {
				HTableDescriptor tagDesc = new HTableDescriptor("TAG");
				tagDesc.addFamily(new HColumnDescriptor("META".getBytes()));
				hbaseAdmin.createTable(tagDesc);
				System.out.println(" setupEnvironment() -> TAG HTable created...");

				HTableDescriptor iovDesc = new HTableDescriptor("IOV");
				iovDesc.addFamily(new HColumnDescriptor("HASH".getBytes()));
				iovDesc.addFamily(new HColumnDescriptor("META".getBytes()));
				hbaseAdmin.createTable(iovDesc);
				System.out.println(" setupEnvironment() -> IOV HTable created...");

				HTableDescriptor plDesc = new HTableDescriptor("PAYLOAD");
				HColumnDescriptor d = new HColumnDescriptor("DATA".getBytes());
				d.setCompressionType(Compression.Algorithm.GZ);
				d.setCompactionCompressionType(Compression.Algorithm.GZ);
				plDesc.addFamily(d);

				plDesc.addFamily(new HColumnDescriptor("META".getBytes()));
				hbaseAdmin.createTable(plDesc);
				System.out.println(" setupEnvironment() -> PAYLOAD HTable created...");
			} catch (IOException e) {
				System.out.println("Exception occured. Details: " + e.toString());
			}

			try {
				Put testTag = new Put(Bytes.toBytes("TEST_TAG"));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("REVISION"), Bytes.toBytes(1));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("REVISION_TIME"),
						Bytes.toBytes(System.currentTimeMillis()));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("COMMENT"),
						Bytes.toBytes("This is the first and only tag for testing"));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("TIME_TYPE"), Bytes.toBytes(1));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("OBJECT_TYPE"),
						Bytes.toBytes("RANDOM"));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("LAST_VALIDATED_TIME"),
						Bytes.toBytes(111));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("END_OF_VALIDITY"),
						Bytes.toBytes(222));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("LAST_SINCE"), Bytes.toBytes(333));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("LAST_SINCE_PID"), Bytes.toBytes(444));
				testTag.add(Bytes.toBytes("META"), Bytes.toBytes("CREATION_TIME"),
						Bytes.toBytes(System.currentTimeMillis()));

				HTable t = new HTable(hbaseConf, "TAG");
				t.put(testTag);
				System.out.println(" setupEnvironment() -> Initial TAG added...");

			} catch (IOException e) {
				System.out.println("Exception occured. Details: " + e.toString());
			}

			System.out.println(" setupEnvironment() -> The environment has been deployed.\n");
		}

		@Override
		protected void destroyEnvironment() {
			System.out.println("------- HBase environment teardown -----------");
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			try {
				hbaseAdmin.disableTable("TAG");
				hbaseAdmin.deleteTable("TAG");
				hbaseAdmin.disableTable("IOV");
				hbaseAdmin.deleteTable("IOV");
				hbaseAdmin.disableTable("PAYLOAD");
				hbaseAdmin.deleteTable("PAYLOAD");
			} catch (IOException e) {
				System.out.println("Exception occured. Details: " + e.toString());
			}
			
			System.out.println(" destroyEnvironment() -> The environment has been destroyed.\n");
		}
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "HBaseDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "HBase clusters. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		/*HBaseEnvironmentDeployer deployer =
				new HBaseEnvironmentDeployer("hb-master-test.cern.ch", "2181",
						"TestHBase", "testUser", "testPass");*/

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// HBase specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (HBase default: 2181)");

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
				HBaseEnvironmentDeployer deployer =
						new HBaseEnvironmentDeployer(optMap.get("HOST"), optMap.get("PORT"),
								optMap.get("DB"), optMap.get("USER"), optMap.get("PASS"));
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

