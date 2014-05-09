package postgresql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import customjdbc.CustomJDBCConfigElement;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class PostgreLOBQueryHandler implements QueryHandler {

	private static Connection connection;
	private static LargeObjectManager lobManager;

	public PostgreLOBQueryHandler(String connectionId)
			throws CustomSamplersException {
		
		connection = CustomJDBCConfigElement.getJDBCConnection(connectionId);
		System.out.println("Fetched: " + connectionId + " ref:" + connection.toString());
		try {
			lobManager = ((org.postgresql.PGConnection)connection).getLargeObjectAPI();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured! Details: " + e.toString());
		}
		if (connection == null)
			throw new CustomSamplersException("JDBCConnection instance with ID: " + connectionId
					+ " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			connection.setAutoCommit(false);
			PreparedStatement ps = connection.prepareStatement("SELECT data FROM LOB_PAYLOAD p, "
					+ "(SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov "
					+ "WHERE p.hash = iov.payload_hash");
			ps.setString(1, tagName);
			ps.setLong(2, since);
			ResultSet rs = ps.executeQuery();

			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					byte[] pl = new byte[object.size()];
					object.read(pl, 0, object.size());
					object.close();
					result.write(pl);
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one payload found for "
							+ "TAG=" + tagName + " SINCE=" + since +" !");
				}
				rs.close();
			} else {

				throw new CustomSamplersException("Payload not found for "
						+ "TAG=" + tagName + " SINCE=" + since +" !");
			}
			ps.close();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString()
					+ " " + e.getErrorCode());
		} catch (IOException e) {
			e.printStackTrace();
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new CustomSamplersException("SQLException occured during close attempt: " + e.toString()
					+ " " + e.getErrorCode());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		writeLOBPayload(metaInfo, payload.toByteArray(), streamerInfo.toByteArray());
		writeIov(metaInfo);
		/*try {
			connection.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("Cannot close connection after INSERT..." + e.toString());
		}*/
		
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			PreparedStatement ps = connection.prepareStatement("SELECT id, data "
					+ "FROM LOB_CHUNK c, (SELECT payload_hash FROM IOV WHERE tag_name=? AND since=?) iov "
					+ "WHERE c.payload_hash = iov.payload_hash");
			ps.setString(1, tagName);
			ps.setLong(2, since);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					byte[] chunk = new byte[object.size()];
					object.read(chunk, 0, object.size());
					object.close();

					ByteArrayOutputStream os = new ByteArrayOutputStream();
					os.write(chunk);
					os.close();
					result.put(rs.getInt("id"), os);
				}
				rs.close();
			} else {
				throw new CustomSamplersException("Payload not found for "
						+ "TAG=" + tagName + " SINCE=" + since +" ! (via chunk read)");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during stream write attempt: " + e.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			writeLOBPayload(metaInfo, new byte[0], new byte[0]);
			writeIov(metaInfo);
			for (int i = 0; i < chunks.size(); ++i) {
				long objectId = lobManager.createLO(LargeObjectManager.READWRITE);
				LargeObject object = lobManager.open(objectId, LargeObjectManager.WRITE);
				object.write(chunks.get(i).toByteArray());
				object.close();

				PreparedStatement ps = connection.prepareStatement("INSERT INTO LOB_CHUNK"
						+ " (PAYLOAD_HASH, CHUNK_HASH, ID, DATA) VALUES (?, ?, ?, ?)");
				ps.setString(1, metaInfo.get("payload_hash"));
				Integer id = i + 1;
				ps.setString(2, metaInfo.get(id.toString()));
				ps.setInt(3, id);
				ps.setLong(4, objectId);
				ps.execute();
				ps.close();
			}
			connection.commit();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	private void writeLOBPayload(HashMap<String, String> metaInfo, byte[] payload, byte[] streamerInfo)
			throws CustomSamplersException {
		try {
			connection.setAutoCommit(false);
			long objectId = lobManager.createLO(LargeObjectManager.READWRITE);
			LargeObject object = lobManager.open(objectId, LargeObjectManager.WRITE);
			object.write(payload);
			object.close();

			PreparedStatement ps = connection.prepareStatement("INSERT INTO LOB_PAYLOAD"
					+ " (HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, CREATION_TIME, CMSSW_RELEASE)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?)");
			ps.setString(1, metaInfo.get("payload_hash"));
			ps.setString(2, metaInfo.get("object_type"));
			ps.setLong(3, objectId);
			ps.setBinaryStream(4, new ByteArrayInputStream(streamerInfo), streamerInfo.length);
			ps.setString(5, metaInfo.get("version"));
			ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			ps.setString(7, metaInfo.get("cmssw_release"));
			ps.execute();
			ps.close();
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new CustomSamplersException("SQLException occured during LOB insert attempt: " + e.toString()
					+ " " + e.getErrorCode()
					+ " " + e.getMessage());
		}
	}

	@SuppressWarnings("unused")
	private byte[] readLOBPayload(String hashKey) throws CustomSamplersException {
		byte[] result = null;
		try {
			PreparedStatement ps = connection.prepareStatement(
					"SELECT DATA FROM LOB_PAYLOAD WHERE HASH=?");
			ps.setString(1, hashKey);
			ResultSet rs = ps.executeQuery();
			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					result = new byte[object.size()];
					object.read(result, 0, object.size());
					object.close();
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one row found with hash="
							+ hashKey + " in LOB_PAYLOAD !");
				}
				rs.close();
			} else {
				throw new CustomSamplersException("The row with hash=" + hashKey
						+ " not found in LOB_PAYLOAD!");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	@SuppressWarnings("unused")
	private void writeLOBChunk(HashMap<String, String> metaInfo, String chunkID, byte[] chunk) 
			throws CustomSamplersException {
		try {
			long objectId = lobManager.createLO(LargeObjectManager.READWRITE);
			LargeObject object = lobManager.open(objectId, LargeObjectManager.WRITE);
			object.write(chunk);
			object.close();
			PreparedStatement ps = connection.prepareStatement("INSERT INTO LOB_CHUNK"
					+ " (PAYLOAD_HASH, CHUNK_HASH, DATA) VALUES (?, ?, ?)");
			ps.setString(1, metaInfo.get("payload_hash"));
			ps.setString(2, metaInfo.get(chunkID));
			ps.setLong(3, objectId);
			ps.execute();
			ps.close();
			connection.commit();
		} catch (SQLException se) {
			throw new CustomSamplersException(
					"SQLException occured during write attempt: " + se.toString());
		}
	}

	@SuppressWarnings("unused")
	private byte[] readLOBChunk(String hashKey, String chunkHashKey) 
			throws CustomSamplersException {
		byte[] result = null;
		try {
			PreparedStatement ps = connection.prepareStatement(
					"SELECT DATA FROM LOB_CHUNK WHERE PAYLOAD_HASH=? AND CHUNK_HASH=?");
			ps.setString(1, hashKey);
			ps.setString(2, chunkHashKey);
			ResultSet rs = ps.executeQuery();

			if (rs != null) {
				int counter = 0;
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					result = new byte[object.size()];
					object.read(result, 0, object.size());
					object.close();
					counter++;
				}
				if (counter > 1) {
					throw new CustomSamplersException("More than one row found with "
							+ "hash=" + hashKey + " and chunk_hash=" + chunkHashKey + " in LOB_CHUNK !");
				}
				rs.close();

			} else {

				throw new CustomSamplersException("The row with hash=" + hashKey
						+ " chunk_hash=" +chunkHashKey + " not found in LOB_CHUNK!");
			}
			ps.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}

	public byte[] readLOBChunks(String hashKey)
			throws CustomSamplersException {
		byte[] result = null;
		try {
			PreparedStatement ps = connection.prepareStatement(
					"SELECT DATA FROM LOB_CHUNK WHERE PAYLOAD_HASH=?");
			ps.setString(1, hashKey);
			ResultSet rs = ps.executeQuery();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			if (rs != null) {
				while(rs.next()) {
					long objectID = rs.getLong(1);
					LargeObject object = lobManager.open(objectID, LargeObjectManager.READ);
					byte[] chunk = new byte[object.size()];
					object.read(chunk, 0, object.size());
					object.close();
					os.write(chunk);
				}
				rs.close();
			} else {
				throw new CustomSamplersException("The rows with hash=" + hashKey + " not found in LOB_CHUNK!");
			}
			ps.close();
			result = os.toByteArray();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString()
					+ " ---- " + e.getSQLState()
					+ " ---- " + e.getErrorCode()
					+ " -----" + e.getLocalizedMessage());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during stream write attempt: " + e.toString());
		}
		return result;
	}

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

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
