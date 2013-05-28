package rsipos.customsamplers;

public class MyTLSingleton {

	private MyTLSingleton() {
		System.out.println("SINGLETON CREATED:" + this.toString());
	}
	
	private static final ThreadLocal<MyTLSingleton> _localStorage = new ThreadLocal<MyTLSingleton>();
	
	protected MyTLSingleton initialValue() {
		return new MyTLSingleton();
	}
	
	public static MyTLSingleton getInstance() {
		return _localStorage.get();
	}
	
}
