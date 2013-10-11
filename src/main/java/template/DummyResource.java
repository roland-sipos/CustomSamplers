package template;

public class DummyResource {

	private String foo = "woof woof";
	private String bar = "foow foow";

	public DummyResource(String foo, String bar) {
		this.foo = foo;
		this.bar = bar;
	}

	public String getFoo() {
		return foo;
	}

	public String getBar() {
		return bar;
	}

	public void setFoo(String newFoo) {
		foo = newFoo;
	}

}
