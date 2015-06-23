package utils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * The QueryHandler interface represents the required communication protocol
 * with a database entity. The implemented classes need to define, how a
 * given PAYLOAD (also if in chunk format) is written into and read from a given database.
 * It may happen that a database support different techniques to R/W binary data.
 * In such case, the packages contain a dedicated QueryHandler implementation for all techniques.
 * */
public interface QueryHandler {

	/** 
	 * The implementation of this method should close all fetched resources that
	 * were used during the handling of a given query.
	 * @throws  CustomSamplersException  if the closing of resources failed. */
	public void closeResources() throws CustomSamplersException;

	/**
	 * The implementation of this method should return a ByteBuffer 
	 * object that contains the data, read from a given database that the
	 * implementation handles.
	 * <p>
	 * @param  tagName  the requested PAYLOAD's TAG_NAME
	 * @param  since  the requested PAYLOAD's SINCE
	 * @return  a ByteBuffer object that holds the BLOB of the PAYLOAD.
	 * @throws  CustomSamplersException  if the reading of the PAYLOAD failed
	 */
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException;

	/**
	 * The implementation of this method should write the meta information and the
	 * data of the PAYLOAD itself into the databases that is handled by the 
	 * implemented class. 
	 * <p>
	 * @param  metaInfo  this map holds the meta-information of the PAYLOAD (SINCE, TAGNAME)
	 * @param  payload  the BLOB of the PAYLOAD to write
	 * @param  streamerInfo  the BLOB of the PAYLOAD's streamer information to write
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
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException;

	/**
	 * The implementation of this method should write a list of ByteArrayOutputStream objects
	 * (where the index indicates the chunk ID), as a representation of chunk ID and chunk 
	 * value pairs. Also the implementation should write the meta information into the database
	 * that is handled by the implemented class.
	 * <p>
	 * @param  metaInfo  the requested PAYLOAD's TAG_NAME
	 * @param  chunks  the chunks of the PAYLOAD to write
	 * @throws  CustomSamplersException  if the writing of the PAYLOAD's chunks failed
	 */
	public void putChunks(HashMap<String, String> metaInfo, List<ByteArrayOutputStream> chunks)
			throws CustomSamplersException;

}

