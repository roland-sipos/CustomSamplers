package utils;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class DeployerOptions {

	/** Possible command line options of the class. */
	private Options options = new Options();

	public DeployerOptions() {
		options.addOption("help", false, "help");
		options.addOption("m", "mode", true, "mode of deployment: deploy, teardown, redeploy (teardown, then deploy)");
		options.addOption("h", "host", true, "hostname or IP of the environment");
		options.addOption("p", "port", true, "port of the host");
		options.addOption("d", "db-instance", true, "name of the database instance, if needed");
		options.addOption("u", "username ", true, "username, if instance needs authentication");
		options.addOption("s", "password", true, "password, if instance needs authentication");
	}

	public void addDeployerOption(String opt, Boolean hasArg, String description) {
		options.addOption(opt, hasArg, description);
	}

	public Options getDeployerOptions() {
		return options;
	}

	public static HashMap<String, String> mapCommandLine(CommandLine cmdl) {
		HashMap<String, String> argMap = new HashMap<String, String>();
		if (cmdl.hasOption("help")) {
			argMap.put("HELP", "Help page requested. Did not touch anything.");
			return argMap;
		}

		if (cmdl.hasOption('m')) {
			argMap.put("MODE", cmdl.getOptionValue('m'));
		} else if (cmdl.hasOption("mode")) {
			argMap.put("MODE", cmdl.getOptionValue("mode"));
		} else {
			argMap.put("HELP", "Mode argument is missing!");
			return argMap;
		}

		if (cmdl.hasOption('h')) {
			argMap.put("HOST", cmdl.getOptionValue('h'));
		} else if (cmdl.hasOption("host")) {
			argMap.put("HOST", cmdl.getOptionValue("host"));
		} else {
			argMap.put("HELP", "Host argumet is missing!");
			return argMap;
		}

		if (cmdl.hasOption('p')) {
			argMap.put("PORT", cmdl.getOptionValue('p'));
		} else if (cmdl.hasOption("port")) {
			argMap.put("PORT", cmdl.getOptionValue("port"));
		} else {
			argMap.put("HELP", "Port argument is missing!");
			return argMap;
		}

		if (cmdl.hasOption('d')) {
			argMap.put("DB", cmdl.getOptionValue('d'));
		} else if (cmdl.hasOption("db-instance")) {
			argMap.put("DB", cmdl.getOptionValue("db-instance"));
		} else {
			argMap.put("HELP", "DB instance argument is missing!");
			return argMap;
		}

		if (cmdl.hasOption('u')) {
			argMap.put("USER", cmdl.getOptionValue('u'));
		} else if (cmdl.hasOption("username")) {
			argMap.put("USER", cmdl.getOptionValue("username"));
		} else {
			argMap.put("USER", "");
		}

		if (cmdl.hasOption('s')) {
			argMap.put("PASS", cmdl.getOptionValue('s'));
		} else if (cmdl.hasOption("password")) {
			argMap.put("PASS", cmdl.getOptionValue("password"));
		} else {
			argMap.put("PASS", "");
		}

		return argMap;
	}

	public static void printHelp(Options depOps, String[] args, String classCmd, String clpHeader) {
		HelpFormatter formatter = new HelpFormatter();
		if (args.length < 1) {
			System.err.println("Arguments are required for deploying anything...\n");
			formatter.printHelp(classCmd, clpHeader, depOps, utils.Constants.SUPPORT_FOOTER);
			return;
		}
	}

}

