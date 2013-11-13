package utils;

public class PaGeS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*getLoc();
		String pagesLocation = this.*/

		String pagesLocation = Thread.currentThread()
				.getContextClassLoader()
				.getResource("resources/PaGeS.py").getPath();
		
		System.out.println(pagesLocation);

	}

}
