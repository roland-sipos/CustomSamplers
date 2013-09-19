package utils;

public abstract class TestEnvironmentDeployer {

	protected String host;
	protected String port;
	protected String databaseName;
	protected String username;
	protected String password;

	public TestEnvironmentDeployer(String host, String port, 
			String databaseName, String username, String password) {
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.username = username;
		this.password = password;
	}

	protected abstract void initialize(); //Initialization of connections.
	protected abstract void tearDown(); //Close connection.
	protected abstract void setupEnvironment(); //Deploy env.
	protected abstract void destroyEnvironment(); // Destroy env.

	public String getHost() { return host; }
	public String getPort() { return port; }
	public String getDatabase() { return databaseName; }
	public String getUsername() { return username; }
	public String getPassword() { return password; }

	protected void checkAndNotify(Boolean failed, String query, String prefix) {
		if (!failed)
			System.out.println(prefix + "The following statement was successfull:\n" + query);
	}

	public void deployTestEnvironment() { 
		initialize();
		setupEnvironment();
		tearDown();
	}

	public void destroyTestEnvironment() {
		initialize();
		destroyEnvironment();
		tearDown();
	}

	public void redeployEnvironment() {
		initialize();
		destroyEnvironment();
		setupEnvironment();
		tearDown();
	}

}
