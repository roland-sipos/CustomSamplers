package example;


/**
 * This class is a demo of how the CustomSamplers extension handles
 * external resources that are fetched from an external context.
 * */
public class DummyResourceHandler {

	/**
	 * This field holds the static external resource object.
	 * */
	private static DummyResource resourceToHandle = null;

	/**
	 * The constructors gets the resource object as a parameter.
	 * 
	 * */
	public DummyResourceHandler(DummyResource resource) {
		resourceToHandle = resource;
	}

	public void howLongDoesThisTake() {
		// TODO: Implement that I want to measure here!
		String foo = resourceToHandle.getFoo();
		String bar = resourceToHandle.getBar();
		if (!foo.equals(bar)) {
			resourceToHandle.setFoo(resourceToHandle.getBar());
		}
	}

}
