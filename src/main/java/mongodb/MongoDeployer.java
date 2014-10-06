package mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

/** 
 * This executable class is capable to deploy a MongoDB collections into a database instance.
 * */
public class MongoDeployer {

	/**
	 * This class is the extended EnvironmentDeployer for MongoDB.
	 * */
	private static class MongoEnvironmentDeployer extends EnvironmentDeployer {

		/** MongoDB client object. */
		MongoClient client = null;
		/** The MongoDB database object. */
		DB mongoDB = null;

		String isSharded = "false";
		
		List<ServerAddress> addresses = null;
		
		/** Constructor
		 * @param host target host for deployment (passed for super EnvironmentDeployer)
		 * @param port port of target host (passed for super)
		 * @param databaseName the name of the database instance on target host (passed for super)
		 * @param username the user's name for target authentication (passed for super)
		 * @param password the user's password for target authentication (passed for super)
		 * */
		public MongoEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password, String sharded) {
			super(host, port, databaseName, username, password);
			isSharded = sharded;
		}

		/** Initialize function for MongoDB. */
		@Override
		protected void initialize() {
			System.out.println("-------- MongoDB connection initialization -------");
			System.out.println(" initialize -> Initialization started...");
			try {
				if (getHost().contains(",")) {
					Integer port = Integer.parseInt(getPort());
					String hosts[] = getHost().split(",");
					addresses = new ArrayList<ServerAddress>();
					for (int i = 0; i < hosts.length; ++i) {
						addresses.add(new ServerAddress(hosts[i], port));
					}
					client = new MongoClient(addresses);
					client.setReadPreference(ReadPreference.nearest());
				} else {
					ServerAddress address = new ServerAddress(getHost(), Integer.parseInt(getPort()));
					client = new MongoClient(address);
				}
			} catch (Exception e) {
				System.out.println(" initialize -> Exception occured: " + e.toString());
			}

			mongoDB = client.getDB(getDatabase());
			boolean auth = mongoDB.isAuthenticated();
			if (!auth) {
				System.out.println(" initialize -> Needs authentication...");
				if (!getUsername().equals("")) {
					auth = mongoDB.authenticate(getUsername(), getPassword().toCharArray());
					if (auth) {
						System.out.println(" initialize -> Authentication successfull...");
					} else {
						System.out.println(" initialize -> Authentication failed...");
					}
				} else {
					System.out.println(" initialize -> Username or passwords is empty!");
				}
			} else {
				System.out.println(" initialize -> Doesn't need to authenticate...");
			}
			System.out.println(" initialize -> Initialization successfull!\n");
		}

		/** Tear-down function for MongoDB. */
		@Override
		protected void tearDown() {
			System.out.println("-------- MongoDB connection teardown ------------");
			System.out.println(" tearDown -> Teardown started...");
			client.close();
			System.out.println(" tearDown -> Teardown successfull!\n");
		}

		/** Environment setup function for MongoDB. */
		@Override
		protected void setupEnvironment() {
			System.out.println("--------- MongoDB environment setup ------------");
			if (isSharded.equals("true")) {
				System.out.println(" setupEnvironment -> Sharding options requested...");
				DB adminDB = client.getDB("admin");
				if (getHost().contains(",")) {
					String hosts[] = getHost().split(",");
					for (int i = 0; i < hosts.length; ++i) {
						DBObject cmd = new BasicDBObject();
						cmd.put("addShard", new String(hosts[i]).concat(":27019"));
						CommandResult res = adminDB.command(cmd);
						System.out.println(" setupEnvironment -> Adding shard: " + hosts[i]
								+ " Result: " + res.getErrorMessage());
					}
				}
			}

			System.out.println(" setupEnvironment -> Enviroment setup started...");
			System.out.println("  -> DBObject options = BasicDBObjectBuilder.start()"
					+ ".add(\"capped\", false).get();");
			DBObject options = BasicDBObjectBuilder.start()
					.add("capped", false).get();
			System.out.println("  -> mongoDB.createCollection(\"TAG\", options);");
			mongoDB.createCollection("TAG", options);
			BasicDBObject tagIdx = new BasicDBObject();
			tagIdx.put("tag", 1);
			System.out.println("  -> Ensuring index on TAG with: " + tagIdx.toString());
			mongoDB.getCollection("TAG").ensureIndex(tagIdx);

			System.out.println("  -> mongoDB.createCollection(\"IOV\", options);");
			mongoDB.createCollection("IOV", options);
			BasicDBObject iovIdx = new BasicDBObject();
			iovIdx.put("tag", 1);
			iovIdx.put("since", 1);
			System.out.println("  -> Ensuring index on IOV with: " + iovIdx.toString());
			mongoDB.getCollection("IOV").ensureIndex(iovIdx);

			System.out.println("  -> mongoDB.createCollection(\"PAYLOAD\", options);");
			mongoDB.createCollection("PAYLOAD", options);
			BasicDBObject plIdx = new BasicDBObject();
			plIdx.put("hash", 1);
			System.out.println("  -> Ensuring index on PAYLOAD with: " + plIdx.toString());
			mongoDB.getCollection("PAYLOAD").ensureIndex(plIdx);
			System.out.println(" --> Adding dummy PAYLOAD...");
			GridFS gridFS = new GridFS(mongoDB, "PAYLOAD");
			GridFSInputFile plGfsFile = gridFS.createFile("This is a dummy payload".getBytes());
			plGfsFile.setFilename("blablahash");
			plGfsFile.save();

			if (isSharded.equals("true")) {
				System.out.println(" setupEnvironment -> Sharding DB and Collection...");
				DB adminDB = client.getDB("admin");

				DBObject enableCmd = new BasicDBObject();
				enableCmd.put("enablesharding", getDatabase());
				CommandResult res = adminDB.command(enableCmd);
				System.out.println(" setupEnvironment -> Enable sharding result: "
						+ res.getErrorMessage());

				DBObject cmd = new BasicDBObject();
				cmd.put("shardCollection", getDatabase().concat(".PAYLOAD.chunks"));
				BasicDBObject keys = new BasicDBObject();
				keys.put("files_id", 1);
				keys.put("n", 1);
				cmd.put("key", keys);
				res = adminDB.command(cmd);
				System.out.println(" setupEnvironment -> Sharding PAYLOAD.chunks result: "
						+ res.getErrorMessage());
			}

			System.out.println(" setupEnvironment -> Environment setup successfull!\n");
		}

		/** Environment destroy function for MongoDB. */
		@Override
		protected void destroyEnvironment() {
			System.out.println("-------- MongoDB environment teardown ------------");
			System.out.println(" setupEnvironment -> Enviroment destroy started...");
			System.out.println("  -> mongoDB.getCollection(\"TAG\").drop();");
			mongoDB.getCollection("TAG").drop();
			System.out.println("  -> mongoDB.getCollection(\"IOV\").drop();");
			mongoDB.getCollection("IOV").drop();
			System.out.println("  -> mongoDB.getCollection(\"PAYLOAD\").drop();");
			mongoDB.getCollection("PAYLOAD").drop();
			System.out.println("  -> mongoDB.getCollection(\"PAYLOAD.files\").drop();");
			mongoDB.getCollection("PAYLOAD.files").drop();
			System.out.println("  -> mongoDB.getCollection(\"PAYLOAD.chunks\").drop();");
			mongoDB.getCollection("PAYLOAD.chunks").drop();
			System.out.println(" setupEnvironment -> Enviroment successfully destroyed!\n");
		}
		
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "MongoDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "MongoDB instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// MongoDB specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (MongoDB default: 27017)");
		depOps.addOption("s", "shard", true, "handle sharding on host (false or true)");

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
			CommandLine line = parser.parse(depOps, args);
			HashMap<String, String> optionMap = DeployerOptions.mapCommandLine(line);

			if (line.hasOption('a')) {
				optionMap.put("SHARD", line.getOptionValue('a'));
			} else if (line.hasOption("shard")) {
				optionMap.put("SHARD", line.getOptionValue("shard"));
			} else {
				optionMap.put("HELP", "Shard argument is missing!");
			}
			if (optionMap.containsKey("HELP")) {
				System.out.println(optionMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				MongoEnvironmentDeployer deployer =
						new MongoEnvironmentDeployer(optionMap.get("HOST"), optionMap.get("PORT"),
								optionMap.get("DB"), optionMap.get("USER"), optionMap.get("PASS"),
								optionMap.get("SHARD"));
				if (optionMap.get("MODE").equals("deploy")) {
					deployer.deployTestEnvironment();
				} else if (optionMap.get("MODE").equals("teardown")) {
					deployer.destroyTestEnvironment();
				} else if (optionMap.get("MODE").equals("redeploy")) {
					deployer.redeployEnvironment();
				} else {
					System.err.println("Unknown deployment mode: " + optionMap.get("MODE"));
					formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
				}
			}

		} catch (ParseException exp) {
			System.err.println("Parsing failed. Details: " + exp.getMessage() + "\n");
			formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
		}

	}
	
}
