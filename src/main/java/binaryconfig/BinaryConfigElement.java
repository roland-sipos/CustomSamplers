package binaryconfig;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import utils.BinaryFileInfo;
import utils.CustomSamplersException;

public class BinaryConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = 5820940444795925355L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	public final static String INPUT_LOCATION = "BinaryConfigElement.inputlocation";
	public final static String BINARY_INFO = "BinaryConfigElement.binaryInfo";
	public final static String ENCODING = "BinaryConfigElement.encoding";
	public final static String ASSIGNMENT_FILE = "BinaryConfigElement.assignmentFile";

	@Override
	public void testEnded() {
		getThreadContext().getVariables().putObject(getBinaryInfo(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public static BinaryFileInfo getBinaryFileInfo(String binaryInfo) throws CustomSamplersException {
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

			BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance(
					getInputLocation(), getAssignmentFile());
			getThreadContext().getVariables().putObject(getBinaryInfo(), binaryInfo);
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
	public String getAssignmentFile() {
		return getPropertyAsString(ASSIGNMENT_FILE);
	}
	public void setAssignmentFile(String assignmentFile) {
		setProperty(ASSIGNMENT_FILE, assignmentFile);
	}

}
