package mongodb;

import java.io.ByteArrayOutputStream;

import org.apache.commons.codec.binary.Base64;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;



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
			String dataBase64 = object.get("data").toString();
			byte[] result = Base64.decodeBase64(dataBase64); //object.get("data").toString().getBytes();
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
			String valueBase64 = new String(Base64.encodeBase64(value));
			chunkObj.put("data", valueBase64);
			collection.insert(chunkObj);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during BSON creation or insertion: " + e.toString());
		}
	}
	
	public void writeFileToMongo(String binaryID, String chunkID, String hash, byte[] fileContent) 
			throws CustomSamplersException {
		String fullColl = collection.getName().concat("GFS");
		try {
			GridFS gridFs = new GridFS(mongo, fullColl);
			// Create GridFS File
			GridFSInputFile gFsFile = gridFs.createFile(fileContent);
			gFsFile.setFilename(chunkID + ".bin");
			gFsFile.save();
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during GridFS create file:" + e.toString());
		}
		// Store the meta, wherever it'll go.
		/*BasicDBObject metaObj = new BasicDBObject();
		metaObj.put("chunkID", chunkID);
		metaObj.put("originalID", binaryID);
		metaObj.put("hash", hash);
		metaObj.put("fileName", chunkID + ".bin");
		try {
			collection.insert(metaObj, WriteConcern.SAFE);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during BSON insertion to GridFS: " + e.toString());
		}*/
	}
	
	public byte[] readFileFromMongo(String fileName) 
			throws CustomSamplersException {
		String fullColl = collection.getName().concat("GFS");
		ByteArrayOutputStream resultOs = new ByteArrayOutputStream();
		try {
			GridFS gridFs = new GridFS(mongo, fullColl);
			GridFSDBFile binaryFile = gridFs.findOne(fileName);
			binaryFile.writeTo(resultOs);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during GridFS file reading: " + e.toString());
		}
		return resultOs.toByteArray();
	}
	
}
