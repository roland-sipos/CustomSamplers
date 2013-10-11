package utils;

public class CustomSamplersException extends Exception {

	private static final long serialVersionUID = 357716820999255104L;

	public CustomSamplersException(String exStr, Exception e) {
		super(e);
		System.err.println("CustomSamplersException -> ".concat(exStr));
		e.printStackTrace();
		// Further stuff to be implemented in order to show the Exception.
	}

	public CustomSamplersException(String exStr) {
		System.err.println(exStr);
	}
	
}

