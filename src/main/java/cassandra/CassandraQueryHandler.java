package cassandra;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class CassandraQueryHandler implements QueryHandler {

	private static Session session;

	public CassandraQueryHandler(String resourceId) throws CustomSamplersException {
		session = CassandraConfigElement.getCassandraSession(resourceId);
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		try {
			PreparedStatement statement = session.prepare(
					"SELECT hash FROM conddb.iov WHERE tag=? AND since=?");
			BoundStatement boundStatement = new BoundStatement(statement);
			ResultSetFuture future = session.executeAsync(boundStatement.bind(tagName, since));
			ResultSet resSet = future.getUninterruptibly();
			List<Row> rows = resSet.all();
			if (rows.size() > 1) {
				throw new CustomSamplersException("More than one hash found for Key:("
						+ tagName + "," + since + ") This should never happen!");
			}
			String hash = rows.get(0).getString("hash");

			statement = session.prepare(
					"SELECT data FROM conddb.payload WHERE hash=?");
			boundStatement = new BoundStatement(statement);
			future = session.executeAsync(boundStatement.bind(hash));
			resSet = future.getUninterruptibly();
			return resSet.one().getBytes("data");
			//result.write(resSet.one().getBytes("data").array());
		} catch (Exception he) {
			throw new CustomSamplersException("Exception occured during read attempt:" + he.toString());
		}
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		try {
			PreparedStatement statement = session.prepare(
					"INSERT INTO conddb.iov (tag, since, hash) VALUES (?, ?, ?);");
			BoundStatement boundStatement = new BoundStatement(statement);
			session.executeAsync(boundStatement.bind(
					metaInfo.get("tag_name"),
					Long.valueOf(metaInfo.get("since")),
					metaInfo.get("payload_hash")));

			statement = session.prepare(
					"INSERT INTO conddb.payload (hash, data) VALUES (?, ?);");
			boundStatement = new BoundStatement(statement);
			session.executeAsync(boundStatement.bind(metaInfo.get("payload_hash"),
					ByteBuffer.wrap(payload.toByteArray()) ));
		} catch (Exception he) {
			throw new CustomSamplersException("Exception occured:" + he.toString());
		}
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException {
		TreeMap<Integer, ByteBuffer> result = new TreeMap<Integer, ByteBuffer>();
		try {
			PreparedStatement statement = session.prepare(
					"SELECT pl_hash FROM conddb.iov WHERE tag=? AND since=?");
			BoundStatement boundStatement = new BoundStatement(statement);
			ResultSetFuture future = session.executeAsync(boundStatement.bind(tagName, since));
			ResultSet resSet = future.getUninterruptibly();
			List<Row> rows = resSet.all();

			/*if (rows.size() > 1) {
				throw new CustomSamplersException("More than one hash found for Key:("
						+ tagName + "," + since + ") This should never happen!");
			}*/

			String plHash = rows.get(0).getString("pl_hash");
			//System.out.println("pl_hash is: " + plHash);

			statement = session.prepare(
					"SELECT data FROM conddb.payload WHERE pl_hash=?");
			boundStatement = new BoundStatement(statement);
			future = session.executeAsync(boundStatement.bind(plHash));//chunkHashes.get(i)));
			resSet = future.getUninterruptibly();
			int idx = 1;
			for (Row row : resSet.all()) {
				result.put(idx, row.getBytes("data"));
				++idx;
			}
		} catch (Exception he) {
			throw new CustomSamplersException("Exception occured during write attempt:" + he.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			List<String> chunkHashes = new ArrayList<String>();
			for (int i = 0; i < chunks.size(); ++i) { 
				chunkHashes.add(i, metaInfo.get(String.valueOf(i+1)));
			}
			String plHash = metaInfo.get("payload_hash");

			PreparedStatement statement = session.prepare(
					"INSERT INTO conddb.iov (tag, since, pl_hash, hash) VALUES (?, ?, ?, ?);");
			BoundStatement boundStatement = new BoundStatement(statement);
			session.executeAsync(boundStatement.bind(
					metaInfo.get("tag_name"),
					Long.valueOf(metaInfo.get("since")),
					plHash,
					chunkHashes));

			/* WARNING -> BatchStatement is not supported in 1.2  Using multiple inserts.*/
			for (int i = 0; i < chunks.size(); ++i) {
				statement = session.prepare(
						"INSERT INTO conddb.payload (pl_hash, hash, data) VALUES (?, ?, ?);");
				boundStatement = new BoundStatement(statement);
				session.executeAsync(boundStatement.bind(
						plHash,
						chunkHashes.get(i),
						ByteBuffer.wrap(chunks.get(i).toByteArray()) ));
			}
		} catch (Exception he) {
			throw new CustomSamplersException("Exception occured:" + he.toString());
		}
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
