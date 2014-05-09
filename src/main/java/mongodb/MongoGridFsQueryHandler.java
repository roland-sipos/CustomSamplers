package mongodb;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import utils.CustomSamplersException;
import utils.QueryHandler;

/**
 * This is the implemented QueryHandler for MongoDB Databases, if content is
 * written through the GridFS API.
 * */
public class MongoGridFsQueryHandler implements QueryHandler {

	/** A MongoDB's DB object, fetched from a MongoConfigElement. */
	private static DB mongo;
	/** A DBCollection object of the IOV Collection in the DB instance. */
	private static DBCollection iovCollection;
	/** The GridFS object, that is defined on the PAYLOAD collection. */
	private static GridFS payloadGridFS;
	
	public MongoGridFsQueryHandler(String databaseName) 
			throws CustomSamplersException {
		mongo = MongoConfigElement.getMongoDB(databaseName);
		iovCollection = mongo.getCollection("IOV");
		payloadGridFS = new GridFS(mongo, "PAYLOAD");
		if (mongo == null)
			throw new CustomSamplersException("MongoDB instance with name: " + databaseName + " was not found in config!");
		if (iovCollection == null)
			throw new CustomSamplersException("Collection IOV not found in the " + databaseName + " database!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			BasicDBObject idToFind = new BasicDBObject();
			idToFind.put("tag", tagName);
			idToFind.put("since", String.valueOf(since));

			BasicDBObject query = new BasicDBObject();
			query.put("_id", idToFind);
			DBCursor cursor = iovCollection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one result found for"
						+ " tagName:" + tagName
						+ " since:" + since + " ! This should never happen!");
			}
			DBObject object = cursor.next();
			GridFSDBFile binaryFile = payloadGridFS.findOne(object.get("hash").toString());
			binaryFile.writeTo(result);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during Mongo GridFS read: "
					+ e.toString());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		try {
			BasicDBObject id = new BasicDBObject();
			id.put("tag", metaInfo.get("tag_name"));
			id.put("since", metaInfo.get("since"));

			BasicDBObject iovDoc = new BasicDBObject();
			iovDoc.put("_id", id);
			iovDoc.put("hash", metaInfo.get("payload_hash"));
			iovCollection.save(iovDoc);

			GridFSInputFile plGfsFile = payloadGridFS.createFile(payload.toByteArray());
			plGfsFile.setFilename(metaInfo.get("payload_hash"));
			plGfsFile.save();
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during MongoDB GridFS write:"
					+ e.toString());
		}
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			BasicDBObject idToFind = new BasicDBObject();
			idToFind.put("tag", tagName);
			idToFind.put("since", String.valueOf(since));

			BasicDBObject query = new BasicDBObject();
			query.put("_id", idToFind);
			DBCursor cursor = iovCollection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one result found for"
						+ " tagName:" + tagName
						+ " since:" + since + " ! This should never happen!");
			}
			DBObject object = cursor.next();
			BasicDBObject hashes = (BasicDBObject)object.get("hashes");
			for (int i = 0; i < hashes.size(); ++i) {
				GridFSDBFile cFile = payloadGridFS.findOne(hashes.getString(String.valueOf(i+1)));
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cFile.writeTo(cBaos);
				result.put(i+1, cBaos);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomSamplersException("Exception occured during MongoDB GridFS chunk read:"
					+ e.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			BasicDBObject id = new BasicDBObject();
			id.put("tag", metaInfo.get("tag_name"));
			id.put("since", metaInfo.get("since"));

			BasicDBObject hashes = new BasicDBObject();
			for (int i = 0; i < chunks.size(); ++i) {
				String cId = String.valueOf(i+1);
				String hash = metaInfo.get(cId);
				hashes.put(cId, hash);
				GridFSInputFile plGfsFile = payloadGridFS.createFile(chunks.get(i).toByteArray());
				plGfsFile.setFilename(hash);
				plGfsFile.save();
			}

			BasicDBObject iovDoc = new BasicDBObject();
			iovDoc.put("_id", id);
			iovDoc.put("hashes", hashes);
			iovCollection.save(iovDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during MongoDB GridFS chunk write:"
					+ e.toString());
		}
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
