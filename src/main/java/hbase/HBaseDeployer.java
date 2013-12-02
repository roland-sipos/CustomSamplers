package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import utils.EnvironmentDeployer;

public class HBaseDeployer {

	private static class HBaseTestEnvironmentDeployer extends EnvironmentDeployer {

		private static Configuration hbaseConf;
		private static HBaseAdmin hbaseAdmin;

		public HBaseTestEnvironmentDeployer(String host, String port,
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
				plDesc.addFamily(new HColumnDescriptor("DATA".getBytes()));
				plDesc.addFamily(new HColumnDescriptor("META".getBytes()));
				hbaseAdmin.createTable(plDesc);
				System.out.println(" setupEnvironment() -> Payload HTable created...");
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HBaseTestEnvironmentDeployer deployer =
				new HBaseTestEnvironmentDeployer("hb-master-test.cern.ch", "2181",
						"TestHBase", "testUser", "testPass");

		//System.out.println("-------- HBase environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- HBase environment teardown -----------");
		//deployer.destroyTestEnvironment();
		System.out.println("-------- HBase environment teardown and setup ------------");
		deployer.redeployEnvironment();
	}

}

