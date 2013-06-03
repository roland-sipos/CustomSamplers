package couchdb;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class CouchQueryHandler implements QueryHandler {

	private static Database couchDB;

	public CouchQueryHandler(String databaseName, String collection) 
			throws CustomSamplersException, NotFoundInDBException {
		couchDB = CouchConfigElement.getCouchDB(databaseName);
		if (couchDB == null)
			throw new NotFoundInDBException("CouchDB Database instance with name: " 
		                                    + databaseName + " was not found in config!");
	}
	
	@Override
	public void writeBinary(String binaryID, String chunkID, String hash, 
			                byte[] fileContent, boolean isSpecial)
			throws CustomSamplersException {
		if (isSpecial) {
			writeBinaryAsAttachment(binaryID, chunkID, hash, fileContent);
		    return;
		}
		Document doc = new Document();
		doc.put("binaryID", binaryID);
		doc.put("chunkID", chunkID);
		String valueBase64 = new Base64().encodeToString(fileContent);
		doc.put("blob", valueBase64);
		try {
			couchDB.saveDocument(doc, hash);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during insert attempt to CouchDB: " 
		                                      + "Details: " + e.toString());
		}
	}

	private void writeBinaryAsAttachment(String binaryID, String chunkID,
			String hash, byte[] fileContent) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial) 
			throws CustomSamplersException {
		Document doc = null;
		try {
			doc = couchDB.getDocument(hash);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during read attempt from CouchDB: "
					                          + "Details: " + e.toString());
		}
		String dataBase64 = doc.getString("blob");
		byte[] result = new Base64().decode(dataBase64);
		return result;
	}

}
