package utils;

/** This abstract class defines a skeleton for the implemented classes that are able
 * to deploy and delete schemas in different databases.*/
public abstract class EnvironmentDeployer {

	/** Host name of the manipulated machine. */
	protected String host;
	/** Port to connect to for the manipulation. */
	protected String port;
	/** The name of the database to look up on the machine. */
	protected String databaseName;
	/** Username for authentication. */
	protected String username;
	/** Password for authentication. */
	protected String password;

	/** The constructor sets up every member field, got as parameters. */
	public EnvironmentDeployer(String host, String port, 
			String databaseName, String username, String password) {
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.username = username;
		this.password = password;

	}

	/** Abstract method for initializing the connection with the machine. */
	protected abstract void initialize(); //Initialization of connections.
	/** Abstract method for closing the connection with the machine. */
	protected abstract void tearDown(); //Close connection.
	/** Abstract method for setting up the test environment on the machine. */
	protected abstract void setupEnvironment(); //Deploy env.
	/** Abstract method for destroying the test environment on the machine. */
	protected abstract void destroyEnvironment(); // Destroy env.

	public String getHost() { return host; }
	public String getPort() { return port; }
	public String getDatabase() { return databaseName; }
	public String getUsername() { return username; }
	public String getPassword() { return password; }

	/** An utility function for printing error on the standard output. */
	protected void checkAndNotify(Boolean failed, String query, String prefix) {
		if (!failed)
			System.out.println(prefix + "The following statement was successfull:\n" + query);
	}

	/** Utility function for deploying test environments. <br>
	 * <p>
	 * Phases: <br>
	 * 1. Calling initialize() to initialize connection. <br>
	 * 2. Calling setupEnvironment() to deploy schema. <br>
	 * 3. Calling tearDown() to close connection. <br>
	 * */
	public void deployTestEnvironment() { 
		initialize();
		setupEnvironment();
		tearDown();
	}

	/** Utility function for deleting test environments. <br>
	 * <p>
	 * Phases: <br>
	 * 1. Calling initialize() to initialize connection. <br>
	 * 2. Calling destroyEnvironment() to delete schema. <br>
	 * 3. Calling tearDown() to close connection. <br>
	 * */
	public void destroyTestEnvironment() {
		initialize();
		destroyEnvironment();
		tearDown();
	}

	/** Utility function for re-deploying test environments. <br>
	 * <p>
	 * Phases: <br>
	 * 1. Calling initialize() to initialize connection. <br>
	 * 2. Calling destroyEnvironment() to delete previous schema. <br> 
	 * 3. Calling setupEnvironment() to deploy schema. <br>
	 * 4. Calling tearDown() to close connection. <br>
	 * */
	public void redeployEnvironment() {
		initialize();
		destroyEnvironment();
		setupEnvironment();
		tearDown();
	}

}
