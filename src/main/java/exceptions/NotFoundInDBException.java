package exceptions;

public class NotFoundInDBException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8527234233667273917L;

	public NotFoundInDBException(String etc_not_found) {
        super(etc_not_found);
    }
}
