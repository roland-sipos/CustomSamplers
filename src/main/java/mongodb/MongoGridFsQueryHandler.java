package mongodb;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import utils.CustomByteArrayOutputStream;
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

	private static int chunkSize = 255; // default

	public MongoGridFsQueryHandler(String connectionId, int cSize) 
			throws CustomSamplersException {
		mongo = MongoConfigElement.getMongoDB(connectionId);
		System.out.println("  -> Using DB: " + mongo.toString());
		payloadGridFS = MongoConfigElement.getGridFS(connectionId);
		System.out.println("  -> Using GridFS: " + payloadGridFS.toString());
		iovCollection = mongo.getCollection("IOV");
		chunkSize = cSize;
		if (mongo == null || payloadGridFS == null)
			throw new CustomSamplersException("MongoDB objects with id: " + connectionId
					+ " were not found in config!");
		if (iovCollection == null)
			throw new CustomSamplersException("Collection IOV not found in the "
					+ connectionId + " database!");
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		try {
			BasicDBObject idxToFind = new BasicDBObject();
			idxToFind.put("tag", tagName);
			idxToFind.put("since", since);
			DBObject iovDoc = iovCollection.findOne(idxToFind);

			GridFSDBFile binaryFile = //payloadGridFS.findOne(iovDoc.get("hash").toString());
					payloadGridFS.findOne(new ObjectId(iovDoc.get("files_id").toString()));
			CustomByteArrayOutputStream cbaos = new CustomByteArrayOutputStream();
			binaryFile.writeTo(cbaos);
			return ByteBuffer.wrap(cbaos.getByteBuffer());
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during Mongo GridFS read: "
					+ e.toString(), e);
		}
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		try {
			GridFSInputFile plGfsFile = payloadGridFS.createFile(payload.toByteArray());
			Object fileID = plGfsFile.getId();
			plGfsFile.setFilename(metaInfo.get("payload_hash"));
			plGfsFile.setChunkSize((int)(chunkSize * 1024));
			plGfsFile.save();

			BasicDBObject iovDoc = new BasicDBObject();
			iovDoc.put("tag", metaInfo.get("tag_name"));
			iovDoc.put("since", Long.valueOf(metaInfo.get("since")));
			iovDoc.put("hash", metaInfo.get("payload_hash"));
			iovDoc.put("files_id", fileID);
			iovCollection.save(iovDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during MongoDB GridFS write:"
					+ e.toString(), e);
		}
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName,
			long since) throws CustomSamplersException {
		throw new CustomSamplersException("Chunk read, using GridFS doesn't make sense. "
				+ "The driver does it for you...");
		/*TreeMap<Integer, ByteBuffer> result = new TreeMap<Integer, ByteBuffer>();
		try {
			BasicDBObject idToFind = new BasicDBObject();
			idToFind.put("tag", tagName);
			idToFind.put("since", String.valueOf(since));

			BasicDBObject query = new BasicDBObject();
			query.put("_id", idToFind);
			DBObject iovObj = iovCollection.findOne(query);

			BasicDBObject hashes = (BasicDBObject) iovObj.get("hashes");
			for (int i = 0; i < hashes.size(); ++i) {
				GridFSDBFile cFile = payloadGridFS.findOne(hashes.getString(String.valueOf(i+1)));
				CustomByteArrayOutputStream cBaos = new CustomByteArrayOutputStream();
				cFile.writeTo(cBaos);
				result.put(i+1, ByteBuffer.wrap(cBaos.getByteBuffer()));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomSamplersException("Exception occured during MongoDB GridFS chunk read:"
					+ e.toString());
		}
		return result;*/
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		throw new CustomSamplersException("Chunk write, using GridFS doesn't make sense. "
				+ "The driver does it for you...");
		/*try {
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
		}*/
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		
	}

}
