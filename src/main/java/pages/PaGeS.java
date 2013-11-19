package pages;

/**
 * This simple class is a wrapper around the PaGeS python script, to make it usable
 * from the Java context.
 * */
public class PaGeS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*getLoc();
		String pagesLocation = this.*/

		String pagesLocation = Thread.currentThread()
				.getContextClassLoader()
				.getResource("pages/PaGeS.py").getPath();
		
		System.out.println(pagesLocation);

	}

}
