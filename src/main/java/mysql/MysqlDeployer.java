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

public class MysqlDeployer {

	private static final String classCmd = "MysqlDeployer [OPTIONS]";
	private static final String clpHeader = "This class helps you to deploy test environments on "
			+ "MySQL instances. For this, one needs to pass connection details of the server.\n"
			+ "The possible arguments are the following:";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();

		Options depOps = new DeployerOptions().getDeployerOptions();
		// MySQL specific options are added manually here:
		depOps.addOption("e", "engine", true, "which MySQL engine to use. (INNODB, MYISAM)");
		depOps.addOption("f", "fork", true, "mark, if this MySQL instance is a special fork. (Mysql, MariaDB)");

		HelpFormatter formatter = new HelpFormatter();
		if (args.length < 1) {
			System.err.println("Arguments are required for deploying anything...\n");
			formatter.printHelp(classCmd, clpHeader, depOps, utils.Constants.supportFooter);
			return;
		}

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine line = parser.parse(depOps, args);
			HashMap<String, String> optionMap = DeployerOptions.mapCommandLine(line);

			if (optionMap.containsKey("HELP")) {
				System.out.println(optionMap.get("HELP") + "\n");
				formatter.printHelp(classCmd, clpHeader, depOps, utils.Constants.supportFooter);
			} else {
				// MySQL specific options are parsed manually here:
				if (line.hasOption('e')) {
					optionMap.put("ENGINE", line.getOptionValue('e'));
				} else if (line.hasOption("engine")) {
					optionMap.put("ENGINE", line.getOptionValue("engine"));
				} else {
					optionMap.put("HELP", "Engine argument is missing!");
				}

				if (line.hasOption('f')) {
					optionMap.put("FORK", line.getOptionValue('f'));
				} else if (line.hasOption("fork")) {
					optionMap.put("FORK", line.getOptionValue("fork"));
				} else {
					optionMap.put("HELP", "Fork argument is missing!");
				}

				MysqlEnvironmentDeployer deployer =
						new MysqlEnvironmentDeployer(optionMap.get("HOST"), optionMap.get("PORT"),
								optionMap.get("DB"), optionMap.get("USER"), optionMap.get("PASS"),
								optionMap.get("ENGINE"), optionMap.get("FORK"), tagList);
				if (optionMap.get("MODE").equals("deploy")) {
					deployer.deployTestEnvironment();
				} else if (optionMap.get("MODE").equals("teardown")) {
					deployer.destroyTestEnvironment();
				} else if (optionMap.get("MODE").equals("redeploy")) {
					deployer.redeployEnvironment();
				} else {
					System.err.println("Unknown deployment mode: " + optionMap.get("MODE"));
					formatter.printHelp(classCmd, clpHeader, depOps, utils.Constants.supportFooter);
				}
			}

		} catch (ParseException exp) {
			System.err.println("Parsing failed. Details: " + exp.getMessage() + "\n");
			formatter.printHelp(classCmd, clpHeader, depOps, utils.Constants.supportFooter);
		}

		/*MysqlEnvironmentDeployer deployer =
				new MysqlEnvironmentDeployer("test-mysql.cern.ch", "3306", 
						"testdb", "testUser", "testPass", "InnoDB", "Mysql", tagList);*/

		//System.out.println("-------- MySQL environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- MySQL environment teardown ----------");
		//deployer.destroyTestEnvironment();
		//System.out.println("------- MySQL environment teardown and setup -----------");
		//deployer.redeployEnvironment();

	}

}

