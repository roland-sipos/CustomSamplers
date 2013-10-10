package template;

public class DummyResourceHandler {

	private static DummyResource resourceToHandle = null;
	
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
