package binaryconfig;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.CustomSamplersException;

/**
 * This class is a JMeter ConfigElement for configuring and storing a BinaryFileInfo
 * instance as a JMeter variable in the JMeterContext.
 * */
public class BinaryConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = 5820940444795925355L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The inputLocation for the BinaryFileInfo instance. */
	public final static String INPUT_LOCATION = "BinaryConfigElement.inputLocation";
	/** The name (ID) of this ConfigElement resource. */
	public final static String BINARY_INFO = "BinaryConfigElement.binaryInfo";
	/** Encoding flag for the BinaryFileInfo. 
	 * @deprecated */
	@Deprecated
	public final static String ENCODING = "BinaryConfigElement.encoding";

	/**
	 * This function looks up the BinaryFileInfo by the given ID, and if found fetches the
	 * instance from the JMeterContext and return it.
	 * 
	 * @param  binaryInfo  the asked ID of the BinaryFileInfo
	 * @return  BinaryFileInfo  the asked BinaryFileInfo
	 * @throws  CustomSamplersException  if the object was not found, or not an instance of BinaryFileInfo
	 * */
	public static BinaryFileInfo getBinaryFileInfo(String binaryInfo)
			throws CustomSamplersException {
		Object fileInfoObject = JMeterContextService.getContext().getVariables().getObject(binaryInfo);
		if (fileInfoObject == null) {
			throw new CustomSamplersException("BinaryFileInfo object is null!");
		}
		else {
			if (fileInfoObject instanceof BinaryFileInfo) {
				return (BinaryFileInfo)fileInfoObject;
			}
			else {
				throw new CustomSamplersException("Casting the object to BinaryFileInfo failed!");
			}
		}
	}

	/** When the test ends, this method removes the BinaryFileInfo object from the JMeterContext. */
	@Override
	public void testEnded() {
		getThreadContext().getVariables().putObject(getBinaryInfo(), null);
	}

	/** The remote version of testEnded function calls the non-remote version, so does the same. */
	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	/**
	 * When the test starts, this ConfigElement puts the BinaryFileInfo singleton into the
	 * JMeterContext, with an ID that is got from the user options. Basically, with the name
	 * of the given ConfigElement.
	 * */
	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
			log.debug("Input location of binary files is: " + getInputLocation());
		}

		if (getThreadContext().getVariables().getObject(getBinaryInfo()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getBinaryInfo() + " has already been defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getBinaryInfo() + " is being defined...");
			}

			BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance(getInputLocation());
			getThreadContext().getVariables().putObject(getBinaryInfo(), binaryInfo);
		}

	}

	/** The remote version of testStarted function calls the non-remote version, so does the same. */
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

	public String getInputLocation() {
		return getPropertyAsString(INPUT_LOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUT_LOCATION, inputLocation);
	}
	public String getBinaryInfo() {
		return getPropertyAsString(BINARY_INFO);
	}
	public void setBinaryInfo(String binaryInfo) {
		setProperty(BINARY_INFO, binaryInfo);
	}
	public String getEncoding() {
		return getPropertyAsString(ENCODING);
	}
	public void setEncoding(String encoding) {
		setProperty(ENCODING, encoding);
	}

}
