package utils;

/**
 * This utility exception is a wrapper around the standard Java Exception class.
 * It offers multiple constructors to pass messages and exceptions and recreate
 * them as an internal CustomSamplers related exception.
 * */
public class CustomSamplersException extends Exception {

	private static final long serialVersionUID = 357716820999255104L;
	//private static final Logger log = LoggingManager.getLoggerForClass();

	/**
	 * This constructor gets an unique message and and exception as a parameter.
	 * As a first step it wraps that exception, then write the exception message
	 * to the standard error output, then also prints the exception's stack trace.
	 * Mainly used in stand-alone Java applications.
	 * 
	 * @param  exStr  additional error message
	 * @param  e  the Exception object to be wrapped
	 * */
	public CustomSamplersException(String exStr, Exception e) {
		super(e);
		// !-> log.error("CustomSamplersException -> ".concat(exStr));
		System.err.println("CustomSamplersException -> ".concat(exStr));
		e.printStackTrace();
		// Further stuff to be implemented in order to show the Exception.
	}

	/**
	 * This constructor gets an unique message as a parameter.
	 * It only print the message to the standard error output, as it mostly called
	 * when the message itself contains the exception as a string.
	 * 
	 * @param  exStr  an error message
	 * */
	public CustomSamplersException(String exStr) {
		System.err.println(exStr);
	}

	/**
	 * This constructor gets an Exception object as a parameter.
	 * It basically wraps the exception, as it mostly used for get-and-pass purposes,
	 * and the Exception's content is not immediate visible.
	 * 
	 * @param  e  the Exception object to be wrapped
	 * */
	public CustomSamplersException(Exception e) {
		super(e);
	}

}

