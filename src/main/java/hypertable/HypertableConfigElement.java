package hypertable;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;

import utils.CustomSamplersException;


public class HypertableConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 7563791847953856903L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String CLUSTERID = "HypertableConfigElement.clusterid";
	public final static String NAMESPACE = "HypertableConfigElement.namespace";
	public final static String HOST = "HypertableConfigElement.host";
	public final static String PORT = "HypertableConfigElement.port";

	public static ThriftClient getHypertableClient(String clusterId)
			throws CustomSamplersException {
		Object client = JMeterContextService.getContext().getVariables().getObject(clusterId);
		if (client == null) {
			throw new CustomSamplersException("Hypertable's ThriftClient object is null!");
		}
		else {
			if (client instanceof ThriftClient) {
				return (ThriftClient)client;
			}
			else {
				throw new CustomSamplersException("Casting the object to ThriftClient failed!");
			}
		}
	}

	public static Long getHypertableNamespace(String clusterId) throws CustomSamplersException {
		Object namespace = JMeterContextService
				.getContext().getVariables().getObject(clusterId.concat("-NS"));
		if (namespace == null) {
			throw new CustomSamplersException("Hypertable's Namespace object is null!");
		}
		else {
			if (namespace instanceof Long) {
				return (Long)namespace;
			}
			else {
				throw new CustomSamplersException("Casting the object to Long failed!");
			}
		}
	}

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
		}

		ThriftClient hyperTClient = null;
		Long hyperTNS = -1L;
		try {
			hyperTClient = ThriftClient.create(getHost(), Integer.valueOf(getPort()));
			if (!hyperTClient.namespace_exists(getNamespace())) {
				String errStr = "Namespace " + getNamespace() + "doesn't exist!";
				log.error(errStr, new CustomSamplersException(errStr));
			} else {
				hyperTNS = hyperTClient.namespace_open(getNamespace());
				log.debug("Namespace " + getNamespace() + " opened.");
			}
		} catch (NumberFormatException e) {
			log.error("NumberFormatException occured. Details: "
					+ e.toString(), new CustomSamplersException(e));
		} catch (TTransportException e) {
			log.error("TTransportException occured. Details: "
					+ e.toString(), new CustomSamplersException(e));
		} catch (TException e) {
			log.error("TException occured. Details: "
					+ e.toString(), new CustomSamplersException(e));
		} catch (ClientException e) {
			log.error("ClientException occured. Details: "
					+ e.toString(), new CustomSamplersException(e));
		}

		if (log.isDebugEnabled()) {
			log.debug("Hypertable ThriftClient: " + hyperTClient.toString());
			log.debug("Hypertable Namespace ID:" + hyperTNS.toString());
		}

		JMeterVariables jMeterVars = getThreadContext().getVariables();
		if (jMeterVars.getObject(getClusterId()) != null 
				|| jMeterVars.getObject(getClusterId().concat("-NS")) != null ) {
			if (log.isWarnEnabled()) {
				log.warn(getClusterId() + " objects are already defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getClusterId() + " objects are being defined...");
			}
			// Put the Hypertable ThriftClient element into the ThreadContext.
			jMeterVars.putObject(getClusterId(), hyperTClient);
			// Put the Hypertable Namespace elements into the ThreadContext.
			jMeterVars.putObject(getClusterId().concat("-NS"), hyperTNS);
		}

	}

	@Override
	public void testStarted(String arg0) {
		testStarted();
	}

	@Override
	public void testEnded() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test ended.");
		}

		Object client = getThreadContext().getVariables().getObject(getClusterId());
		Object namespace = getThreadContext().getVariables().getObject(getClusterId().concat("-NS"));

		if (client == null || namespace == null) {
			log.error("Some of the Hypertable objects are null!");
		} else {
			
			Long ns = -1L;
			if (namespace instanceof Long) {
				ns = (Long)namespace;
				
				if (client instanceof ThriftClient) {
					ThriftClient htCli = (ThriftClient)client;
					try {
						htCli.namespace_close(ns);
					} catch (ClientException e) {
						log.error("ClientException occured. Details: " + e.toString());
					} catch (TException e) {
						log.error("TException occured. Details: " + e.toString());
					}
				} else {
					log.error("Casting the client object to ThriftClient failed!");
				}
			} else {
				log.error("Casting the namespace object to Long failed!");
			}
		}
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	@Override
	public void addConfigElement(ConfigElement arg0) {

	}

	@Override
	public boolean expectsModification() {
		return false;
	}

	public String getTitle() {
		return this.getName();
	}

	public String getClusterId() {
		return getPropertyAsString(CLUSTERID);
	}
	public void setClusterId(String clusterId) {
		setProperty(CLUSTERID, clusterId);
	}

	public String getNamespace() {
		return getPropertyAsString(NAMESPACE);
	}
	public void setNamespace(String namespace) {
		setProperty(NAMESPACE, namespace);
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

}
