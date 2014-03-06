package couchdb;

import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.ektorp.AttachmentInputStream;
import org.ektorp.CouchDbConnector;
import org.ektorp.DbPath;
import org.ektorp.PurgeResult;
import org.ektorp.Revision;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;

/** 
 * This executable class is capable to deploy test environments into CouchDB database instances.
 * */
public class CouchDeployer {

	/**
	 * This class is the extended EnvironmentDeployer for CouchDB.
	 * */
	private static class CouchEnvironmentDeployer extends EnvironmentDeployer {

		/** Standard CouchDB database instance object. */
		StdCouchDbInstance dbInstance = null;
		/** CouchDB connector object. */
		CouchDbConnector couch = null;

		/** Constructor
		 * @param host target host for deployment (passed for super EnvironmentDeployer)
		 * @param port port of target host (passed for super)
		 * @param databaseName the name of the database instance on target host (passed for super)
		 * @param username the user's name for target authentication (passed for super)
		 * @param password the user's password for target authentication (passed for super)
		 * */
		public CouchEnvironmentDeployer(String host, String port,
				String databaseName, String username, String password) {
			super(host, port, databaseName, username, password);
		}

		/** Initialize function for CouchDB. */
		@Override
		protected void initialize() {
			System.out.println("-------- CouchDB connection initialization -------");
			System.out.println(" initialize -> Initializing database connection...");
			String host = getHost();
			/** Host name needs http prefix. Fast check, and fix if needed. */
			if (!getHost().startsWith("http://")) {
				System.out.println(" initialize -> Host does not have \"http://\" prefix... Correcting.");
				host = "http://".concat(getHost());
			}
			/** Create the DBInstance object. */
			try {
				HttpClient client = new StdHttpClient.Builder()
						.url(host.concat(":").concat(getPort()))
						.connectionTimeout(50000)
						.build();
				dbInstance = new StdCouchDbInstance(client);
			} catch (MalformedURLException e) {
				System.out.println(" initialize() -> MalformedURLException occured... " + e.getMessage());
				e.printStackTrace();
			}
			System.out.println(" initialize -> Initialization successfull.\n");
		}

		/** Teardown function for CouchDB. */
		@Override
		protected void tearDown() {
			System.out.println("-------- CouchDB connection teardown ------------");
			System.out.println(" tearDown -> Cleaning up connection...");
			System.out.println(" tearDown -> Shutting down HTTP client through DBInstance...");
			dbInstance.getConnection().shutdown();
			System.out.println(" tearDown -> Connection cleaned up.\n");
		}

		/** This function fast checks if the database exists. 
		 * If it's not, the function creates it and get the connector to it.
		 * */
		private void setConnector() {
			System.out.println(" setConnector() -> Looking up available databases...");
			List<String> dbs = dbInstance.getAllDatabases();
			System.out.println(" setConnector() -> Found databases: " + dbs.toString());
			if (!dbs.contains(getDatabase())) {
				System.out.println(" setConnector() -> Target database not found! Creating it...");
				dbInstance.createDatabase(new DbPath(super.getDatabase()));
				couch = dbInstance.createConnector(getDatabase(), Boolean.TRUE);
			} else {
				couch = dbInstance.createConnector(getDatabase(), Boolean.FALSE);
			}
		}

		/** Environment setup function for CouchDB. */
		@Override
		protected void setupEnvironment() {
			System.out.println("--------- CouchDB environment setup ------------");
			setConnector();

			/** Write a dummy document into the database. */
			Map<String, Object> fooDoc = new HashMap<String, Object>();
			fooDoc.put("_id", "foo");
			fooDoc.put("content", "bar");
			System.out.println(" setupEnvironment() -> Writing initial doc: " + fooDoc.toString());
			couch.create(fooDoc);
			System.out.println(" setupEnvironment() -> Writing small attachment for doc...");
			/** Write a dummy attachment for the previously created document. */
			couch.createAttachment("foo", couch.getCurrentRevision("foo"), 
					new AttachmentInputStream("data", new ByteArrayInputStream(
							new String("bar").getBytes()), "application/octet-stream"));
			System.out.println(" setupEnvironment() -> Environment set up successfull.\n");
		}

		/** Environment destroy function for CouchDB. */
		@Override
		protected void destroyEnvironment() {
			System.out.println("-------- CouchDB environment teardown ------------");
			setConnector();
			System.out.println(" destroyEnvironment() -> Destroying environment...");
			List<String> ids = couch.getAllDocIds();
			System.out.println("  -> Number of documents to be purged: " + ids.size());

			/** First, we delete every attachment of every document and it's revisions ... */
			int docNum = 0;
			for (int i = 0; i < ids.size(); ++i) {
				String id = ids.get(i);
				List<Revision> revs = couch.getRevisions(id);
				for (int j = 0; j < revs.size(); ++j) {
					try {
						// Try to purge attachments also!
						couch.deleteAttachment(id, revs.get(j).getRev(), "data");
						++docNum;
					} catch (Exception e) {
						// If some revision doesn't have attachment, just move on...
						continue;
					}
				}
			}
			System.out.println("  -> Deleted attachments: " + docNum);

			/** ... then we purge the documents also.*/
			Map<String, List<String>> revsToPurge = new HashMap<String, List<String>>();
			for (int i = 0; i < ids.size(); ++i) {
				// the map contains revisions by doc id to purge
				String id = ids.get(i);
				List<String> revsAsStr = new ArrayList<String>();
				List<Revision> revs = couch.getRevisions(id);
				for (int j = 0; j < revs.size(); ++j) {
					revsAsStr.add(revs.get(j).getRev());
				}
				revsToPurge.put(id, revsAsStr);
			}
			PurgeResult res = couch.purge(revsToPurge);
			System.out.println("  -> Purge result: " + res.getPurged().keySet().size() + " docs, "
					+ res.getPurged().entrySet().size() + " revisions purged.");

			/** Finally, also the database is removed. */
			System.out.println("  -> Deleting database: " + super.getDatabase());
			dbInstance.deleteDatabase(super.getDatabase());
			System.out.println(" destroyEnvironment() -> Environment destroyed.\n");
		}
	}

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "CouchDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "CouchDB instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		Options depOps = new DeployerOptions().getDeployerOptions();
		// CouchDB specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (CouchDB default: 5984)");

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
				CouchEnvironmentDeployer deployer =
						new CouchEnvironmentDeployer(optionMap.get("HOST"), optionMap.get("PORT"),
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
