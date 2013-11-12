package utils;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The QueryHandler interface represents the required communication protocol
 * with a database entity. Each implemented class needs to define, how a
 * given PAYLOAD (also if in chunk format) is written into and read from a given database.
 *
 * @category Utilities
 * */
public interface QueryHandler {

	/**
	 * The implementation of this method should return a ByteArrayOutputStream 
	 * object that contains the data, read from a given database that the
	 * implementation handles.
	 * <p>
	 * @param  tagName  the requested PAYLOAD's TAG_NAME
	 * @param  since  the requested PAYLOAD's SINCE
	 * @return  a ByteArrayOutputStream object that holds the BLOB of the PAYLOAD.
	 * @throws  CustomSamplersException  if the reading of the PAYLOAD failed
	 */
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException;

	/**
	 * The implementation of this method should write the meta information and the
	 * data of the PAYLOAD itself into the databases that is handled by the 
	 * implemented class. 
	 * <p>
	 * @param  metaInfo  this map holds the meta-information of the PAYLOAD (SINCE, TAGNAME)
	 * @param  payload  the BLOB of the PAYLOAD to write
	 * @throws  CustomSamplersException  if the writing of the PAYLOAD failed
	 */
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException;

	/**
	 * The implementation of this method should return a mapping of Integers and 
	 * ByteArrayOutputStream objects, as a representation of chunk ID and chunk value pairs.
	 * The mapping is read from the database that is handled by the implemented class.
	 * <p>
	 * @param  tagName  the requested PAYLOAD's TAG_NAME
	 * @param  since  the requested PAYLOAD's SINCE
	 * @return  a map that holds chunk ID - chunk value pairs
	 * @throws  CustomSamplersException  if the reading of the PAYLOAD's chunks failed
	 */
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException;

	/**
	 * The implementation of this method should write a list of ByteArrayOutputStream objects
	 * (where the index indicates the chunk ID), as a representation of chunk ID and chunk 
	 * value pairs. Also the implementation should write the meta information into the database
	 * that is handled by the implemented class.
	 * <p>
	 * @param  tagName  the requested PAYLOAD's TAG_NAME
	 * @param  since  the requested PAYLOAD's SINCE
	 * @throws  CustomSamplersException  if the writing of the PAYLOAD's chunks failed
	 */
	public void putChunks(HashMap<String, String> metaInfo, List<ByteArrayOutputStream> chunks)
			throws CustomSamplersException;

	/*public void readWith(BinaryFileInfo binaryInfo, SampleResult res,
			HashMap<String, Boolean> options);*/
	
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

