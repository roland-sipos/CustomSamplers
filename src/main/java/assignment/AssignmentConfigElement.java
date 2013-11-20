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

/**
 * This class is a JMeter ConfigElement for configuring and storing an Assignment
 * instance as a JMeter variable in the JMeterContext.
 * */
public class AssignmentConfigElement extends AbstractTestElement
implements ConfigElement, TestStateListener, TestBean {

	/** Generated UID. */
	private static final long serialVersionUID = -4529760142362713038L;
	/** Static logger instance from JMeter. */
	private static final Logger log = LoggingManager.getLoggerForClass();

	/** The name (ID) of this ConfigElement resource. */
	private static final String ASSIGNMENT_INFO = "AssignmentConfigElement.assignmentInfo";
	/** The full path of the Assignment XML configuration file for this resource. */
	private static final String ASSIGNMENT_FILE = "AssignmentConfigElement.assignmentFile";
	/** The associated BinaryFileInfo instance for this resource. */
	private static final String BINARY_INFO = "AssignmentConfigElement.binaryInfo";
	/** The assignment mode that this resource need to utilize. */
	private static final String ASSIGNMENT_MODE = "AssignmentConfigElement.assignmentMode";

	/**
	 * This function looks up the Assignment by the given ID, and if found fetches the
	 * instance from the JMeterContext and return it.
	 * 
	 * @param  assignmentInfo  the asked ID of the Assignment
	 * @return  Assignment  the asked Assignment
	 * @throws  CustomSamplersException  if the object was not found, or not an instance of Assignment
	 * */
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
				throw new CustomSamplersException("Casting the object to Assignment failed!");
			}
		}
	}

	/** When the test ends, this method removes the Assignment object from the JMeterContext. */
	@Override
	public void testEnded() {
		getThreadContext().getVariables().putObject(getAssignmentInfo(), null);
	}

	/** The remote version of testEnded function calls the non-remote version, so does the same. */
	@Override
	public void testEnded(String arg0) {
		testEnded();
	}

	/**
	 * When the test starts, this ConfigElement puts the Assignment object into the JMeterContext,
	 * with an ID that is got from the user options. Basically, with the name of the given ConfigElement.
	 * */
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
				log.error("Could not create Assignment object, due to unable to get BinaryFileInfo"
						+ " from BinaryConfigElement with name: " + getBinaryInfo()
						+ " Details: " + e.toString());
			}
			
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
