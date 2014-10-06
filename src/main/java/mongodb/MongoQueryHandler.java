package mongodb;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import utils.CustomSamplersException;
import utils.QueryHandler;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * This is the implemented QueryHandler for MongoDB Databases, if content is
 * written as standard BSON.
 * */
public class MongoQueryHandler implements QueryHandler {

	private static DB mongo;
	private static DBCollection iovCollection;
	private static DBCollection payloadCollection;

	public MongoQueryHandler(String databaseName)
			throws CustomSamplersException {
		mongo = MongoConfigElement.getMongoDB(databaseName);
		iovCollection = mongo.getCollection("IOV");
		payloadCollection = mongo.getCollection("PAYLOAD");
		if (mongo == null)
			throw new CustomSamplersException("MongoDB instance with name: " 
					+ databaseName + " was not found in config!");
		if (iovCollection == null || payloadCollection == null)
			throw new CustomSamplersException("Some of the collections not found in the " 
					+ databaseName + " database!");
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		try {
			BasicDBObject idToFind = new BasicDBObject();
			idToFind.put("tag", tagName);
			idToFind.put("since", String.valueOf(since));

			BasicDBObject query = new BasicDBObject();
			query.put("_id", idToFind);
			DBCursor cursor = iovCollection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one IOV found for"
						+ " tagName:" + tagName
						+ " since:" + since + " ! This should never happen!");
			}
			DBObject object = cursor.next();
			
			query.put("_id", object.get("hash").toString());
			cursor = payloadCollection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one PAYLOAD found for"
						+ " tagName:" + tagName
						+ " since:" + since + " ! This should never happen!");
			}
			object = cursor.next();
			return ByteBuffer.wrap((byte[])object.get("data"));
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during Mongo GridFS read: "
					+ e.toString());
		}
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		try {
			BasicDBObject id = new BasicDBObject();
			id.put("tag", metaInfo.get("tag_name"));
			id.put("since", metaInfo.get("since"));

			BasicDBObject iovDoc = new BasicDBObject();
			iovDoc.put("_id", id);
			iovDoc.put("hash", metaInfo.get("payload_hash"));
			iovCollection.save(iovDoc);

			BasicDBObject plDoc = new BasicDBObject();
			plDoc.put("_id", metaInfo.get("payload_hash"));
			plDoc.put("data", payload.toByteArray());
			payloadCollection.save(plDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during MongoDB write:"
					+ e.toString());
		}
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException {
		TreeMap<Integer, ByteBuffer> result = new TreeMap<Integer, ByteBuffer>();
		try {
			BasicDBObject idToFind = new BasicDBObject();
			idToFind.put("tag", tagName);
			idToFind.put("since", String.valueOf(since));

			BasicDBObject query = new BasicDBObject();
			query.put("_id", idToFind);
			DBCursor cursor = iovCollection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one IOV found for"
						+ " tagName:" + tagName
						+ " since:" + since + " ! This should never happen!");
			}
			DBObject object = cursor.next();

			BasicDBObject hashes = (BasicDBObject)object.get("hashes");
			for (int i = 0; i < hashes.size(); ++i) {
				query.put("_id", hashes.getString(String.valueOf(i+1)));
				BasicDBObject chunk = (BasicDBObject)payloadCollection.findOne(query);
				result.put(i+1, ByteBuffer.wrap( (byte[])chunk.get("data") ));
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
			BasicDBObject hashes = new BasicDBObject();
			BasicDBObject chunk = new BasicDBObject();
			for (int i = 0; i < chunks.size(); ++i) {
				String cId = String.valueOf(i+1);
				String hash = metaInfo.get(cId);
				hashes.put(cId, hash);

				chunk.put("_id", hash);
				chunk.put("data", chunks.get(i));
				payloadCollection.save(chunk);
			}

			BasicDBObject id = new BasicDBObject();
			id.put("tag", metaInfo.get("tag_name"));
			id.put("since", metaInfo.get("since"));
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
