package mongodb;

import java.io.File;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
	
	public void writeBinaryToMongo(String binaryID, String chunkID, String hash, byte[] value)  
			throws CustomSamplersException {
		try {
			BasicDBObject chunkObj = new BasicDBObject();
			chunkObj.put("chunkID", chunkID);
			chunkObj.put("originalID", binaryID);
			chunkObj.put("hash", hash);
			chunkObj.put("data", value);
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
