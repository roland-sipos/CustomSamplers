package assignment;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryconfig.BinaryConfigElement;

import utils.CustomSamplersException;


public class AssignmentConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	private static final long serialVersionUID = -4529760142362713038L;
	private static final Logger log = LoggingManager.getLoggerForClass();

	private static final String ASSIGNMENT_INFO = "AssignmentConfigElement.assignmentInfo";
	private static final String ASSIGNMENT_FILE = "AssignmentConfigElement.assignmentFile";
	private static final String BINARY_INFO = "AssignmentConfigElement.binaryInfo";
	private static final String ASSIGNMENT_MODE = "AssignmentConfigElement.assignmentMode";

	public static Assignment getAssignments(String assignmentInfo) throws CustomSamplersException {
		Object assignmentInfoObject =
				JMeterContextService.getContext().getVariables().getObject(assignmentInfo);
		if (assignmentInfoObject == null) {
			throw new CustomSamplersException("Assignment object is null!");
		}
		else {
			if (assignmentInfoObject instanceof Assignment) {
				return (Assignment)assignmentInfoObject;
			}
			else {
				throw new CustomSamplersException("Casting the object to BinaryFileInfo failed!");
			}
		}
	}

	@Override
	public void testEnded() {
		getThreadContext().getVariables().putObject(getAssignmentInfo(), null);
	}

	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	public String getTitle() {
		return this.getName();
	}

	@Override
	public void testStarted() {
		if (log.isDebugEnabled()) {
			log.debug(getTitle() + " test started...");
			log.debug("Input location of assignment file is: " + getAssignmentFile());
		}

		if (getThreadContext().getVariables().getObject(getAssignmentInfo()) != null) {
			if (log.isWarnEnabled()) {
				log.warn(getAssignmentInfo() + " has already been defined!");
			}
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug(getAssignmentInfo() + " is being defined...");
			}

			try {
				int numOfThreads = getThreadContext().getThreadGroup().getNumberOfThreads();
				Assignment assignment = new Assignment(getAssignmentFile(), getAssignmentMode(),
						numOfThreads, BinaryConfigElement.getBinaryFileInfo(getBinaryInfo()));
				getThreadContext().getVariables().putObject(getAssignmentInfo(), assignment);
			} catch (CustomSamplersException e) {
				log.error("Could not get BinaryFileInfo from BinaryConfigElement with name: "
						+ getBinaryInfo() + " Details: " + e.toString());
			}
			
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

	public String getAssignmentInfo() {
		return getPropertyAsString(ASSIGNMENT_INFO);
	}
	public void setAssignmentInfo(String assignmentInfo) {
		setProperty(ASSIGNMENT_INFO, assignmentInfo);
	}

	public String getAssignmentFile() {
		return getPropertyAsString(ASSIGNMENT_FILE);
	}
	public void setAssignmentFile(String assignmentFile) {
		setProperty(ASSIGNMENT_FILE, assignmentFile);
	}

	public String getAssignmentMode() {
		return getPropertyAsString(ASSIGNMENT_MODE);
	}
	public void setAssignmentMode(String assignmentMode) {
		setProperty(ASSIGNMENT_MODE, assignmentMode);
	}

	public String getBinaryInfo() {
		return getPropertyAsString(BINARY_INFO);
	}
	public void setBinaryInfo(String binaryInfo) {
		setProperty(BINARY_INFO, binaryInfo);
	}


}
