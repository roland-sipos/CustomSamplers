package mongodb;

import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

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

		/** Constructor
		 * @param host target host for deployment (passed for super EnvironmentDeployer)
		 * @param port port of target host (passed for super)
		 * @param databaseName the name of the database instance on target host (passed for super)
		 * @param username the user's name for target authentication (passed for super)
		 * @param password the user's password for target authentication (passed for super)
		 * */
		public MongoEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
		}

		/** Initialize function for MongoDB. */
		@Override
		protected void initialize() {
			System.out.println("-------- MongoDB connection initialization -------");
			System.out.println(" initialize -> Initialization started...");
			try {
				ServerAddress address = new ServerAddress(getHost(), Integer.parseInt(getPort()));
				client = new MongoClient(address);
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
			System.out.println(" setupEnvironment -> Enviroment setup started...");
			System.out.println("  -> DBObject options = BasicDBObjectBuilder.start()"
					+ ".add(\"capped\", true) "
					+ ".add(\"size\", 5242880)"
					+ ".add(\"max\", 1000).get();");
			DBObject options = BasicDBObjectBuilder.start()
					.add("capped", true)
					.add("size", 5242880)
					.add("max", 1000).get();
			System.out.println("  -> mongoDB.createCollection(\"TAG\", options);");
			mongoDB.createCollection("TAG", options);
			System.out.println("  -> mongoDB.createCollection(\"IOV\", options);");
			mongoDB.createCollection("IOV", options);
			System.out.println("  -> mongoDB.createCollection(\"PAYLOAD\", options);");
			mongoDB.createCollection("PAYLOAD", options);
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
		Options depOps = new DeployerOptions().getDeployerOptions();
		// MongoDB specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (MongoDB default: 27017)");

		HelpFormatter formatter = new HelpFormatter();
		if (args.length < 1) {
			System.err.println("Arguments are required for deploying anything...\n");
			formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			return;
		}

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine line = parser.parse(depOps, args);
			HashMap<String, String> optionMap = DeployerOptions.mapCommandLine(line);

			if (optionMap.containsKey("HELP")) {
				System.out.println(optionMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				MongoEnvironmentDeployer deployer =
						new MongoEnvironmentDeployer(optionMap.get("HOST"), optionMap.get("PORT"),
								optionMap.get("DB"), optionMap.get("USER"), optionMap.get("PASS"));
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
