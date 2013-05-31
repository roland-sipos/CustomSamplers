package couchdb;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Session;

import exceptions.CustomSamplersException;

public class CouchConfigElement extends AbstractTestElement
	implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -6669728766687401677L;
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public final static String HOST = "CouchConfigElement.host";
	public final static String PORT = "CouchConfigElement.port";
	public final static String DATABASE = "CouchConfigElement.database";
	public final static String USERNAME = "CouchConfigElement.username";
	public final static String PASSWORD = "CouchConfigElement.password";
	
	
	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}
		getThreadContext().getVariables().putObject(getDatabase(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static Database getCouchDB(String database) throws CustomSamplersException {
		Object couch = JMeterContextService.getContext().getVariables().getObject(database);
		if (couch == null) {
			throw new CustomSamplersException("CouchDB object is null!");
		}
		else {
			if (couch instanceof Database) {
				return (Database)couch;
			}
			else {
				throw new CustomSamplersException("Casting the object to (CouchDB) Database failed!");
			}
		}
	}
	
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(this.getName() + " testStarted()");
		}
		
		if (getThreadContext().getVariables().getObject(getDatabase()) != null) {
			log.warn(getDatabase() + " has already initialized!");
		} else {
			if (log.isDebugEnabled()) {
				log.debug(getDatabase() + " is being initialized ...");
			}
			
			Session session = new Session(getHost(), Integer.parseInt(getPort()));
			Database couch = session.getDatabase(getDatabase());
 			getThreadContext().getVariables().putObject(getDatabase(), couch);
		}
	}

	@Override
	public void testStarted(String arg0) {
		testStarted();
	}

	@Override
	public void addConfigElement(ConfigElement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean expectsModification() {
		// TODO Auto-generated method stub
		return false;
	}


	public String getTitle() {
		return this.getName();
	}
	
	public String getHost() {
		return getPropertyAsString(HOST);
	}

	public void setHost(String host) {
		setProperty(HOST, host);
	}
	
	public String getPort() {
		return getPropertyAsString(PORT);
	}

	public void setPort(String port) {
		setProperty(PORT, port);
	}
	
	public String getDatabase() {
		return getPropertyAsString(DATABASE);
	}

	public void setDatabase(String database) {
		setProperty(DATABASE, database);
	}
	
	public String getUsername() {
		return getPropertyAsString(USERNAME);
	}

	public void setUsername(String username) {
		setProperty(USERNAME, username);
	}
	
	public String getPassword() {
		return getPropertyAsString(PASSWORD);
	}

	public void setPassword(String password) {
		setProperty(PASSWORD, password);
	}
	
}
