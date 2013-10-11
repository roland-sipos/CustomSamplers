package voldemort;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.CustomSamplersException;
import utils.QueryHandler;

import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

public class VoldemortQueryHandler implements QueryHandler {

	private static StoreClient<String, byte[]> voldClient;

	public VoldemortQueryHandler(String database, String store) 
			throws CustomSamplersException {
		voldClient = VoldemortConfigElement.getVoldemortClient(database, store);
		if (voldClient == null)
			throw new CustomSamplersException("Voldemort Client instance with name: " + database 
					+ " store: " + store + " was not found in config!");
	}

	//@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial)
					throws CustomSamplersException {
		Versioned<byte[]> vValue = new Versioned<byte[]>(fileContent);
		voldClient.put(hash, vValue);
	}

	//@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash,
			boolean isSpecial) throws CustomSamplersException {
		Versioned<byte[]> vValue = voldClient.get(hash);
		return vValue.getValue();
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	/*@Override
	public void putChunk(HashMap<String, String> metaInfo, Integer chunkID,
			ByteArrayOutputStream chunk) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}*/

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}


}
