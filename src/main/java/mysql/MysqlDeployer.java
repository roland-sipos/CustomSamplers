package mysql;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import utils.DeployerOptions;
import utils.TagList;

/** 
 * This executable class is capable to deploy a MySQL schema into a database instance.
 * */
public class MysqlDeployer {

	/** The name of this class for the command line parser. */
	private static final String CLASS_CMD = "MysqlDeployer [OPTIONS]";
	/** The help header for the command line parser. */
	private static final String CLP_HEADER = "This class helps you to deploy test environments on "
			+ "MySQL instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args command line arguments, parsed by utils.DeployerOptions.
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();

		/** Get a basic apache.cli Options from DeployerOptions. */
		Options depOps = new DeployerOptions().getDeployerOptions();
		// MySQL specific options are added manually here:
		depOps.addOption("p", "port", true, "port of the host (MySQL default: 3306)");
		depOps.addOption("e", "engine", true, "which MySQL engine to use. (INNODB, MYISAM)");
		depOps.addOption("f", "fork", true, "mark, if this MySQL instance is a special fork. (Mysql, MariaDB)");

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

			// MySQL specific options are parsed manually here:
			if (cl.hasOption('e')) {
				optMap.put("ENGINE", cl.getOptionValue('e'));
			} else if (cl.hasOption("engine")) {
				optMap.put("ENGINE", cl.getOptionValue("engine"));
			} else {
				optMap.put("HELP", "Engine argument is missing!");
			}

			if (cl.hasOption('f')) {
				optMap.put("FORK", cl.getOptionValue('f'));
			} else if (cl.hasOption("fork")) {
				optMap.put("FORK", cl.getOptionValue("fork"));
			} else {
				optMap.put("HELP", "Fork argument is missing!");
			}

			if (optMap.containsKey("HELP")) {
				System.out.println(optMap.get("HELP") + "\n");
				formatter.printHelp(CLASS_CMD, CLP_HEADER, depOps, utils.Constants.SUPPORT_FOOTER);
			} else {
				/** Create an environment deployer with the parsed arguments. */
				MysqlEnvironmentDeployer deployer =
						new MysqlEnvironmentDeployer(optMap.get("HOST"), optMap.get("PORT"),
								optMap.get("DB"), optMap.get("USER"), optMap.get("PASS"),
								optMap.get("ENGINE"), optMap.get("FORK"), tagList);
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

