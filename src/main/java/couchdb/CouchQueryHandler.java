package couchdb;

import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.jcouchdb.db.Database;
import org.jcouchdb.document.BaseDocument;
import org.jcouchdb.util.Base64Util;

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
	
	private BaseDocument createDocFrom(String binaryID, String chunkID, String hash) {
		BaseDocument doc = new BaseDocument();
		doc.setId(hash);
		doc.setProperty("binaryID", binaryID);
		doc.setProperty("chunkID", chunkID);
		return doc;
	}
	
	@Override
	public void writeBinary(String binaryID, String chunkID, String hash, 
			                byte[] fileContent, boolean isSpecial)
			throws CustomSamplersException {
		if (isSpecial) {
			writeBinaryAsAttachment(binaryID, chunkID, hash, fileContent);
		    return;
		}
		BaseDocument doc = createDocFrom(binaryID, chunkID, hash);
		String valueBase64 = Base64Util.encodeBase64(fileContent);
		doc.setProperty("blob", valueBase64);
		try {
			couchDB.createOrUpdateDocument(doc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during insert attempt to CouchDB: " 
		                                      + "Details: " + e.toString());
		}
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial) 
			throws CustomSamplersException {
		if (isSpecial) {
			return readBinaryAsAttachment(binaryID, chunkID, hash);
		}
		BaseDocument doc = null;
		try {
			doc = couchDB.getDocument(BaseDocument.class, hash);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during read attempt from CouchDB: "
					                          + "Details: " + e.toString());
		}
		String dataBase64 = (String) doc.getProperty("blob");
		byte[] result = Base64.decodeBase64(dataBase64);
		return result;
	}

	private void writeBinaryAsAttachment(String binaryID, String chunkID, String hash, byte[] fileContent) 
			throws CustomSamplersException {
		BaseDocument doc = createDocFrom(binaryID, chunkID, hash);
		try {
			couchDB.createOrUpdateDocument(doc);
			couchDB.createAttachment(doc.getId(), doc.getRevision(), 
					"blob", "application/octet-stream", fileContent);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during insert attempt to CouchDB: " 
		                                      + "Details: " + e.toString());
		}
	}
	
	private byte[] readBinaryAsAttachment(String binaryID, String chunkID, String hash) 
			throws CustomSamplersException {
		byte[] result = null;
		try {
			result = couchDB.getAttachment(hash, "blob");
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during read attempt from CouchDB: "
					                          + "Details: " + e.toString());
		}
		return result;
	}
	
}
