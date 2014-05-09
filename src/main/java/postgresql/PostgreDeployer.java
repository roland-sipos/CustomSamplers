package postgresql;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import utils.DeployerOptions;
import utils.EnvironmentDeployer;
import utils.TagList;

/** 
 * This executable class is capable to deploy a relational schema into a PostgreSQL database instance.
 * */
public class PostgreDeployer {

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "PostgreDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "PostgreSQL instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// PostgreSQL specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (PostgreSQL default: 5432)");
		depOps.addOption("a", "api", true, "which PostgreSQL API to use. (DEFAULT, LOBAPI)");

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

			// PostgreSQL specific options are parsed manually here:
			if (cl.hasOption('a')) {
				optMap.put("API", cl.getOptionValue('a'));
			} else if (cl.hasOption("api")) {
				optMap.put("API", cl.getOptionValue("api"));
			} else {
				optMap.put("HELP", "API argument is missing!");
			}

			if (optMap.containsKey("HELP")) {
				System.out.println(optMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				EnvironmentDeployer deployer = null;
				if (optMap.get("API").equals("DEFAULT")) {
					deployer = new PostgreEnvironmentDeployer(optMap.get("HOST"), optMap.get("PORT"),
							optMap.get("DB"), optMap.get("USER"), optMap.get("PASS"), tagList);
				} else if (optMap.get("API").equals("LOBAPI")) {
					deployer = new PostgreLOBEnvironmentDeployer(optMap.get("HOST"), optMap.get("PORT"),
							optMap.get("DB"), optMap.get("USER"), optMap.get("PASS"), tagList);
				} else {
					System.err.println("Unknown API: " + optMap.get("API"));
					formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
				}

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
