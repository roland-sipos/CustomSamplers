package utils;

import java.util.HashMap;


public interface QueryHandler {
	
	public void writeBinary(String binaryID, String chunkID, String hash, 
			                byte[] fileContent, boolean isSpecial) throws CustomSamplersException;
	
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial)
			throws CustomSamplersException;

	/*public void writePayload(HashMap<String, String> metaMap, byte[] payload, boolean isSpecial)
		throws CustomSamplersException;*/
	
}
