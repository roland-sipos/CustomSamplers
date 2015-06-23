package assignment;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import binaryinfo.BinaryFileInfo;

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
	/** The full path of the Assignment XML configuration input file for this resource. */
	private static final String ASSIGNMENT_INPUT_FILE = "AssignmentConfigElement.assignmentInputFile";
	/** The full path of the Assignment XML configuration output file for this resource.*/
	private static final String ASSIGNMENT_OUTPUT_FILE = "AssignmentConfigElement.assignmentOutputFile";
	/** The inputLocation for the BinaryFileInfo instance. */
	public final static String INPUT_LOCATION = "BinaryConfigElement.inputLocation";
	/** The number of assignments to be generated. */
	public final static String NUMBER_OF_ASSIGNMENTS = "BinaryConfigElement.numberOfAssignments";
	/** Encoding flag for the BinaryFileInfo. */
	public final static String ENCODING = "BinaryConfigElement.encoding";
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
			log.debug("Input location of assignment file is: " + getAssignmentInputFile());
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
				int numOfThreads = Integer.valueOf(getNumberOfAssignments());
						//getThreadContext().getVariables().get("numberOfThreads"));
				Assignment assignment =
						new Assignment(getAssignmentInputFile(), getAssignmentOutputFile(),
								getAssignmentMode(), numOfThreads, getEncoding(),
								BinaryFileInfo.getInstance(getInputLocation()));
				getThreadContext().getVariables().putObject(getAssignmentInfo(), assignment);
			} catch (Exception e) {
				log.error("Could not create Assignment object, due to unable to get BinaryFileInfo"
						+ " for inputLocation: " + getInputLocation()
						+ " Details: " + e.getMessage());
				e.printStackTrace();
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

	public String getAssignmentInputFile() {
		return getPropertyAsString(ASSIGNMENT_INPUT_FILE);
	}
	public void setAssignmentInputFile(String assignmentInputFile) {
		setProperty(ASSIGNMENT_INPUT_FILE, assignmentInputFile);
	}

	public String getAssignmentOutputFile() {
		return getPropertyAsString(ASSIGNMENT_OUTPUT_FILE);
	}
	public void setAssignmentOutputFile(String assignmentOutputFile) {
		setProperty(ASSIGNMENT_OUTPUT_FILE, assignmentOutputFile);
	}

	public String getInputLocation() {
		return getPropertyAsString(INPUT_LOCATION);
	}
	public void setInputLocation(String inputLocation) {
		setProperty(INPUT_LOCATION, inputLocation);
	}

	public String getNumberOfAssignments() {
		return getPropertyAsString(NUMBER_OF_ASSIGNMENTS);
	}

	public void setNumberOfAssignments(String numberOfAssignments) {
		setProperty(NUMBER_OF_ASSIGNMENTS, numberOfAssignments);
	}

	public String getEncoding() {
		return getPropertyAsString(ENCODING);
	}

	public void setEncoding(String encoding) {
		setProperty(ENCODING, encoding);
	}

	public String getAssignmentMode() {
		return getPropertyAsString(ASSIGNMENT_MODE);
	}
	public void setAssignmentMode(String assignmentMode) {
		setProperty(ASSIGNMENT_MODE, assignmentMode);
	}

}
