package utils;

import java.util.HashMap;


public interface QueryHandler {

	@Deprecated
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial) throws CustomSamplersException;

	@Deprecated
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial)
			throws CustomSamplersException;


	// The implemented function writes a PAYLOAD to the DB based on all it's meta and key.
	public void writePayload(HashMap<String, String> metaMap,
			byte[] payload, byte[] streamerInfo,
			boolean isSpecial) throws CustomSamplersException;

	// The implemented function reads a PAYLOAD based on the HASH key.
	public byte[] readPayload(String hashKey, boolean isSpecial)
			throws CustomSamplersException;


	// The implemented function writes a CHUNK into the database.
	public void writeChunk(HashMap<String, String> metaInfo, String chunkID, byte[] chunk, Boolean isSpecial)
			throws CustomSamplersException;

	// The implemented function reads a CHUNK based on the PAYLOAD_HASH and CHUNK_HASH attributes.
	public byte[] readChunk(String hashKey, String chunkHashKey, boolean isSpecial)
			throws CustomSamplersException;


	// The implemented function should write an IOV to the database. (Composite key + Payload hash.)
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException;

	// The implemented function should pass a PayloadHash based on IOV keys (TAG_NAME and SINCE).
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException;


	// The implemented function should write a TAG to the DB. (All meta + NAME as key.)
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException;

	// The implemented function reads a TAG based on the NAME key.
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException;

}

