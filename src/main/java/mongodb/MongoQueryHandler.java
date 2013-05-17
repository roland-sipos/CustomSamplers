package mongodb;

import java.io.File;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONObject;
import org.bson.types.Binary;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;

public class MongoQueryHandler {

	private static DB mongo;
	private static DBCollection collection;
	
	public MongoQueryHandler(String databaseName, String collectionName) 
			throws CustomSamplersException, NotFoundInDBException {
		mongo = MongoConfigElement.getMongoDB(databaseName);
		collection = mongo.getCollection(collectionName);
		if (mongo == null)
			throw new NotFoundInDBException("MongoDB instance with name: " + databaseName + " was not found in config!");
		if (collection == null)
			throw new NotFoundInDBException("Collection not found in the " + mongo.getName() + " database!");
	}
	
	public byte[] readBinaryFromMongo(String binaryID, String chunkID, String hash)
			throws CustomSamplersException {
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("chunkID", chunkID);
			query.put("originalID", binaryID);
			query.put("hash", hash);
			DBCursor cursor = collection.find(query);
			if (cursor.count() > 1) {
				throw new CustomSamplersException("More than one result found. This should never happen!");
			}
			DBObject object = cursor.next();
			System.out.println("Cursor result: " + cursor.toString());
			Binary resultBin = (Binary) object.get("data");
			byte[] result = resultBin.getData();
			System.out.println(" DATA:" + resultBin.toString());
			return result;
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during BSON reading: " + e);
		}
	}
	
	public void writeBinaryToMongo(String binaryID, String chunkID, String hash, byte[] value)  
			throws CustomSamplersException {
		try {
			BasicDBObject chunkObj = new BasicDBObject();
			chunkObj.put("chunkID", chunkID);
			chunkObj.put("originalID", binaryID);
			chunkObj.put("hash", hash);
			Binary binValue = new Binary(value);
			chunkObj.put("data", binValue);
			collection.insert(chunkObj);
			//collection.save(chunkObj);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during BSON creation or insertion: " + e.toString());
		}
	}
	
	public void writeFileToMongo(File file, BasicDBObject metaInfo) 
			throws CustomSamplersException {
		String fullColl = collection.getName().concat("-fs");
		GridFS gridFs = new GridFS(mongo, fullColl);
		try {
			// Create GridFS File
			GridFSInputFile gFsFile = gridFs.createFile(file);
			gFsFile.setFilename(file.getName());
			gFsFile.save();
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during GridFS create file:" + e.toString());
		}
		// Store the meta, wherever it'll go.
		try {
			collection.insert(metaInfo, WriteConcern.SAFE);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during BSON insertion to GridFS: " + e.toString());
		}
	}
	
}
