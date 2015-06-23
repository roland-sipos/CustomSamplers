package mysql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import customjdbc.CustomJDBCConfigElement;
import utils.CustomSamplersException;
import utils.QueryHandler;

/**
 * This is the implemented QueryHandler for MySQL Databases.
 * */
public class MysqlQueryHandler implements QueryHandler {

	/** The JDBC Connection object, fetched from a CustomJDBCConfigElement. */
	private static Connection connection;

	/** The constructor receives the ID of the Connection resource, that is looked up in
	 * the JMeterContext and fetched from the CustomJDBCConfigElement.
	 * 
	 * @param  connectionId  the ID of a CustomJDBCConfigElement's resource
	 * @param ifNewConnection 
	 * @throws  CustomSamplersException  if the CustomJDBCConfigElement could not fetch the resource
	 * */
	public MysqlQueryHandler(String connectionId) 
			throws CustomSamplersException {
		connection = CustomJDBCConfigElement.getJDBCConnection(connectionId);
		if (connection == null)
			throw new CustomSamplersException("JDBCConnection instance with ID: "
					+ connectionId + " was not found in config!");
	}

	/**
	 * This function sends the read query for the MySQL database, that is the following: <br>
	 * SELECT data <br>
	 * FROM PAYLOAD p, (SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov <br>
	 * WHERE p.hash = iov.payload_hash <br>
	 * <p>
	 * The values for tag_name and since are got from the parameters, and the result set
	 * contains the binary content of the asked PAYLOAD, in the DATA column in a single row.
	 * 
	 * @param  tagName  the TAG_NAME value for the query
	 * @param  since  the SINCE value for the query
	 * @return  ByteArrayOutputStream  the binary content of the PAYLOAD
	 * @throws  CustomSamplersException  if an error occurred while executing the query
	 * */
	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		ByteBuffer result = null;
		try {
			PreparedStatement ps = connection.prepareStatement("SELECT data FROM PAYLOAD p, "
					+ "(SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov "
					+ "WHERE p.hash = iov.payload_hash");
			ps.setString(1, tagName);
			ps.setLong(2, since);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				//int counter = 0;
				while(rs.next()) {
					result = ByteBuffer.wrap(rs.getBytes("data"));
					//counter++;
				}
				/*if (counter > 1) {
					throw new CustomSamplersException("More than one payload found for "
							+ "TAG=" + tagName + " SINCE=" + since +" !");
				}*/
				rs.close();
			} else {
				throw new CustomSamplersException("Payload not found for "
						+ "TAG=" + tagName + " SINCE=" + since +" !");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	/**
	 * This function sends the read query for the MySQL database, that is the following: <br>
	 * SELECT id, data <br>
	 * FROM CHUNK c, (SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov <br>
	 * WHERE c.payload_hash = iov.payload_hash <br>
	 * <p>
	 * The values for tag_name and since are got from the parameters, and the result set
	 * contains the binary content of the asked PAYLOAD, in the ID and DATA columns where a
	 * single row represent a CHUNK id - CHUNK binary content pair.
	 * 
	 * @param  tagName  the TAG_NAME value for the query
	 * @param  since  the SINCE value for the query
	 * @return  ByteArrayOutputStream  the binary content of the PAYLOAD
	 * @throws  CustomSamplersException  if an error occurred while executing the query
	 * */
	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException {
		TreeMap<Integer, ByteBuffer> result = new TreeMap<Integer, ByteBuffer>();
		try {
			PreparedStatement ps = connection.prepareStatement("SELECT id, data "
					+ "FROM CHUNK c, (SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov "
					+ "WHERE c.payload_hash = iov.payload_hash");
			ps.setString(1, tagName);
			ps.setLong(2, since);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				while(rs.next()) {
					result.put(rs.getInt("id"), ByteBuffer.wrap(rs.getBytes("data")));
				}
				rs.close();
			} else {
				throw new CustomSamplersException("Payload not found for "
						+ "TAG=" + tagName + " SINCE=" + since +" ! (via chunk read)");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	/**
	 * This method defines how a PAYLOAD is written into a MySQL database. First, it writes
	 * the PAYLOAD itself into the database (calling writePayload), and then writes the IOV
	 * (calling writeIov). 
	 * 
	 * @param  metaInfo  the PAYLOAD's meta information to write
	 * @param  payload  the PAYLOAD's binary content to write
	 * @param  streamerInfo  the PAYLOAD's streamer info to write
	 * @throws  CustomSamplersException  if an error occurred during calling the functions
	 * */
	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		writePayload(metaInfo, payload.toByteArray(), streamerInfo.toByteArray());
		writeIov(metaInfo);
	}

	/**
	 * This method defines how a PAYLOAD's CHUNKs are written into a MySQL database. First,
	 * it writes the PAYLOAD's meta information and then writes the IOV related data. Finally,
	 * it writes every CHUNK into the database with the following query: <br>
	 * 
	 * INSERT INTO `CHUNK` (`PAYLOAD_HASH`, `CHUNK_HASH`, `ID`, `DATA`) VALUES (?, ?, ?, ?) <br>
	 * <p>
	 * The values for the query are got from the parameters.
	 * 
	 * @param  metaInfo  the PAYLOAD's meta information to write
	 * @param  chunks  the list of PAYLOAD's CHUNKs' binary content to write
	 * @throws  CustomSamplersException  if an error occurred during calling the functions
	 * */
	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			writePayload(metaInfo, new byte[0], new byte[0]);
			writeIov(metaInfo);

			for (int i = 0; i < chunks.size(); ++i) {
				PreparedStatement ps = connection.prepareStatement("INSERT INTO `CHUNK`"
						+ " (`PAYLOAD_HASH`, `CHUNK_HASH`, `ID`, `DATA`) VALUES (?, ?, ?, ?)");
				ps.setString(1, metaInfo.get("payload_hash"));
				Integer id = i + 1;
				ps.setString(2, metaInfo.get(id.toString()));
				ps.setInt(3, id);
				ps.setBinaryStream(4, new ByteArrayInputStream(chunks.get(i).toByteArray()));
				ps.execute();
				ps.close();
			}
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	/** An old way to handle chunks with this QueryHandler. Will be removed in a later update.
	 * @deprecated*/
	@Deprecated
	public byte[] readChunk(String hashKey, String chunkHashKey)
			throws CustomSamplersException {
		byte[] result = null;
		try {
			PreparedStatement ps = connection.prepareStatement(
					"SELECT `DATA` FROM `CHUNK` WHERE `PAYLOAD_HASH`=? AND `CHUNK_HASH`=?");
			ps.setString(1, hashKey);
			ps.setString(2, chunkHashKey);
			ResultSet rs = ps.executeQuery();

			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					result = rs.getBytes("DATA");
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one row found with "
							+ "hash=" + hashKey + " and chunk_hash=" + chunkHashKey + " in CHUNK !");
				}
				rs.close();

			} else {

				throw new CustomSamplersException("The row with hash=" + hashKey
						+ " chunk_hash=" +chunkHashKey + " not found in CHUNK!");
			}

			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	/**
	 * This method defines how a PAYLOAD is written into a MySQL database, with the following query: <br>
	 * INSERT INTO PAYLOAD <br>
	 *   (HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, CREATION_TIME, CMSSW_RELEASE) <br>
	 *   VALUES (?, ?, ?, ?, ?, ?, ?) <br>
	 * <p>
	 * The values for the query are got from the parameters.
	 * 
	 * @param  metaInfo  the PAYLOAD's meta information to write
	 * @param  payload  the binary content of the PAYLOAD
	 * @param  streamerInfo  the binary content of the PAYLOAD's streamer info
	 * @throws  CustomSamplersException  if an error occurred during calling the functions
	 * */
	public void writePayload(HashMap<String, String> metaMap, byte[] payload, byte[] streamerInfo)
			throws CustomSamplersException {
		try {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO PAYLOAD"
					+ " (HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, CREATION_TIME, CMSSW_RELEASE)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?)");
			ps.setString(1, metaMap.get("payload_hash"));
			ps.setString(2, metaMap.get("object_type"));
			ps.setBinaryStream(3, new ByteArrayInputStream(payload));
			ps.setBinaryStream(4, new ByteArrayInputStream(streamerInfo));
			ps.setString(5, metaMap.get("version"));
			ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			ps.setString(7, metaMap.get("cmssw_release"));
			ps.execute();
			ps.close();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	/**
	 * This method defines how an IOV is written into a MySQL database, with the following query: <br>
	 * INSERT INTO IOV (TAG_NAME, SINCE, PAYLOAD_HASH, INSERT_TIME) VALUES (?, ?, ?, ?) <br>
	 * <p>
	 * The values for the query are got from the parameters.
	 * 
	 * @param  keyAndMetaMap  the PAYLOAD's meta information to write
	 * @throws  CustomSamplersException  if an error occurred during calling the functions
	 * */
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException {
		try {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO IOV"
					+ " (TAG_NAME, SINCE, PAYLOAD_HASH, INSERT_TIME) VALUES (?, ?, ?, ?)");
			ps.setString(1, keyAndMetaMap.get("tag_name"));
			ps.setLong(2, Long.parseLong(keyAndMetaMap.get("since")));
			ps.setString(3, keyAndMetaMap.get("payload_hash"));
			ps.setDate(4, new Date(System.currentTimeMillis()));
			ps.execute();
			ps.close();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	/**
	 * This method defines how an IOV is read from a MySQL database, with the following query: <br>
	 * SELECT PAYLOAD_HASH FROM `IOV` WHERE TAG_NAME=? AND SINCE=? <br>
	 * <p>
	 * The values for the query are got from the parameters.
	 * 
	 * @param  keyMap  the PAYLOAD's meta information that contains the TAG_NAME and SINCE
	 * @return  String  the hash of the PAYLOAD
	 * @throws  CustomSamplersException  if an error occurred during calling the functions
	 * */
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException {
		String result = null;
		try {
			PreparedStatement ps = connection.prepareStatement("SELECT PAYLOAD_HASH FROM `IOV`"
					+ " WHERE TAG_NAME=? AND SINCE=?");
			ps.setString(1, keyMap.get("tag_name"));
			ps.setLong(2, Long.parseLong(keyMap.get("since")));
			ResultSet rs = ps.executeQuery();

			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					result = rs.getString("PAYLOAD_HASH");
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one row found with "
							+ "tag_name=" + keyMap.get("tag_name")
							+ " and since=" + keyMap.get("since")
							+ " in PAYLOAD !");
				}
				rs.close();

			} else {

				throw new CustomSamplersException("The row with"
						+ " tag_name=" + keyMap.get("tag_name")
						+ " and since=" + keyMap.get("since")
						+ " is not found in the database!");
			}

			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	/** An implementation to write TAGs with this QueryHandler. Will be removed in a later update. */
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException {
		try {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO `TAG`"
					+ " (NAME, REVISION, REVISION_TIME, COMMENT, TIME_TYPE, OBJECT_TYPE,"
					+ " LAST_VALIDATED, END_OF_VALIDITY, LAST_SINCE, LAST_SINCE_PID, CREATION_TIME)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			ps.setString(1, metaMap.get("name"));
			ps.setInt(2, Integer.parseInt(metaMap.get("revision")));
			ps.setDate(3, new Date(Long.parseLong(metaMap.get("revision_time"))));
			ps.setString(4, metaMap.get("comment"));
			ps.setInt(5, Integer.parseInt(metaMap.get("time_type")));
			ps.setString(6, metaMap.get("object_type"));
			ps.setInt(7, Integer.parseInt(metaMap.get("last_validated")));
			ps.setInt(8, Integer.parseInt(metaMap.get("end_of_validity")));
			ps.setLong(9, Long.parseLong(metaMap.get("last_since")));
			ps.setInt(10, Integer.parseInt(metaMap.get("last_since_pid")));
			ps.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	/** An implementation to read TAGs with this QueryHandler. Will be removed in a later update. */
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException {
		HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			PreparedStatement ps = connection.prepareStatement("SELECT REVISION, REVISION_TIME,"
					+ " COMMENT, TIME_TYPE, OBJECT_TYPE, LAST_VALIDATED, END_OF_VALIDITY,"
					+ " LAST_SINCE, LAST_SINCE_PID, CREATION_TIME"
					+ " FROM `TAG` WHERE NAME=?");
			ps.setString(1, tagKey);
			ResultSet rs = ps.executeQuery();

			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					result.put("revision", rs.getObject("REVISION"));
					result.put("revision_time", rs.getObject("REVISION_TIME"));
					result.put("comment", rs.getObject("COMMENT"));
					result.put("time_type", rs.getObject("TIME_TYPE"));
					result.put("object_type", rs.getObject("OBJECT_TYPE"));
					result.put("last_validated", rs.getObject("LAST_VALIDATED"));
					result.put("end_of_validity", rs.getObject("END_OF_VALIDITY"));
					result.put("last_since", rs.getObject("LAST_SINCE"));
					result.put("last_since_pid", rs.getObject("LAST_SINCE_PID"));
					result.put("creation_time", rs.getObject("CREATION_TIME"));
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one row found with"
							+ " name=" + tagKey + " in TAG !");
				}
				rs.close();

			} else {

				throw new CustomSamplersException("The row with"
						+ " name=" + tagKey + " is not found in the database!");
			}

			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}

