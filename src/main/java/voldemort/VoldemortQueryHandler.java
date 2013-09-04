package voldemort;

import java.util.HashMap;

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

	@Override
	public void writePayload(HashMap<String, String> metaMap, byte[] payload,
			byte[] streamerInfo, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readPayload(HashMap<String, String> metaMap, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

}
