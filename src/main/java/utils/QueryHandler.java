package utils;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface QueryHandler {

	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException;
	
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException;

	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException;

	public void putChunks(HashMap<String, String> metaInfo, List<ByteArrayOutputStream> chunks)
			throws CustomSamplersException;
	
	/*public void putChunk(HashMap<String, String> metaInfo, Integer chunkID,
			ByteArrayOutputStream chunk)
					throws CustomSamplersException;*/

	/*// The implemented function writes a PAYLOAD to the DB based on all it's meta and key.
	public void writePayload(HashMap<String, String> metaInfo,
			byte[] payload, byte[] streamerInfo, boolean isSpecial)
					throws CustomSamplersException;

	// The implemented function reads a PAYLOAD based on the HASH key.
	public byte[] readPayload(String hashKey, boolean isSpecial)
			throws CustomSamplersException;

	// The implemented function writes a CHUNK into the database.
	public void writeChunk(HashMap<String, String> metaInfo, String chunkID,
			byte[] chunk, Boolean isSpecial)
					throws CustomSamplersException;

	// The implemented function reads a CHUNK based on the PAYLOAD_HASH and CHUNK_HASH attributes.
	public byte[] readChunk(String hashKey, String chunkHashKey, boolean isSpecial)
			throws CustomSamplersException;

	// The implemented function reads all of the CHUNKS of a given PAYLOAD_HASH.
	public byte[] readChunks(String hashKey, boolean isSpecial)
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
			throws CustomSamplersException;*/

}

