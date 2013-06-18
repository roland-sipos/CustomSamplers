package utils;

public abstract class TestEnvironmentDeployer {
	
	protected String host;
	protected String port;
	protected String databaseName;
	protected String entityName;
	protected String username;
	protected String password;
	
	public TestEnvironmentDeployer(String host, String port, 
			                       String databaseName, String entityName,
			                       String username, String password) {
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
		this.entityName = entityName;
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
	public String getEntity() { return entityName; }
	public String getUsername() { return username; }
	public String getPassword() { return password; }
	
	public void deployTestEnv() { 
		initialize();
		setupEnvironment();
		tearDown();
	}
	
	public void destroyTestEnv() {
		initialize();
		destroyEnvironment();
		tearDown();
	}
	
}
