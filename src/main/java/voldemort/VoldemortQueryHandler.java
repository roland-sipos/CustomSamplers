package voldemort;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

public class VoldemortQueryHandler implements QueryHandler {
	
	private static StoreClient<String, byte[]> voldClient;
	
	public VoldemortQueryHandler(String database, String store) 
			throws CustomSamplersException, NotFoundInDBException {
		voldClient = VoldemortConfigElement.getVoldemortClient(database, store);
		if (voldClient == null)
			throw new NotFoundInDBException("Voldemort Client instance with name: " + database 
					                        + " store: " + store + " was not found in config!");
	}

	@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial)
			throws CustomSamplersException {
		Versioned<byte[]> vValue = new Versioned<byte[]>(fileContent);
		voldClient.put(hash, vValue);
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash,
			boolean isSpecial) throws CustomSamplersException {
		Versioned<byte[]> vValue = voldClient.get(hash);
		return vValue.getValue();
	}

}
