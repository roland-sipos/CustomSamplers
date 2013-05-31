package utils;

import exceptions.CustomSamplersException;

public interface QueryHandler {
	
	public void writeBinary(String binaryID, String chunkID, String hash, 
			                byte[] fileContent, boolean isSpecial) throws CustomSamplersException;
	
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial)
			throws CustomSamplersException;
	
}
