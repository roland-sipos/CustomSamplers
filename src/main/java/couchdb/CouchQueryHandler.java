package couchdb;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.ektorp.CouchDbConnector;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class CouchQueryHandler implements QueryHandler {

	private static CouchDbConnector couchDB;

	public CouchQueryHandler(String databaseName) 
			throws CustomSamplersException, NotFoundInDBException {
		couchDB = CouchConfigElement.getCouchDB(databaseName);
		if (couchDB == null)
			throw new NotFoundInDBException("CouchDB Database instance with name: " 
					+ databaseName + " was not found in config!");
	}

	/*private BaseDocument createDocFrom(String binaryID, String chunkID, String hash) {
		BaseDocument doc = new BaseDocument();
		doc.setId(hash);
		BaseDocument doc2 = new BaseDocument();
		doc.setProperty("subdoc", doc2);
		doc.setProperty("binaryID", binaryID);
		doc.setProperty("chunkID", chunkID);
		return doc;
	}

	//@Override
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

	//@Override
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
	}*/

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			BaseDocument doc = couchDB.getDocument(BaseDocument.class,
					tagName.concat("_").concat(String.valueOf(since)));
			String dataBase64 = (String) doc.getProperty("data");
			result.write(Base64.decodeBase64(dataBase64));
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during read attempt from CouchDB! "
					+ "Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		try {
			BaseDocument plDoc = new BaseDocument();
			plDoc.setId(metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since")));
			plDoc.setProperty("payload_hash", metaInfo.get("payload_hash"));
			plDoc.setProperty("object_type", metaInfo.get("object_type"));
			plDoc.setProperty("version", metaInfo.get("version"));
			plDoc.setProperty("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.setProperty("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.setProperty("streamer_info", "streamer_info");
			String valueBase64 = Base64Util.encodeBase64(payload.toByteArray());
			plDoc.setProperty("data", valueBase64);
			couchDB.createOrUpdateDocument(plDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			BaseDocument doc = couchDB.getDocument(BaseDocument.class,
					tagName.concat("_").concat(String.valueOf(since)));
			
			int cNo = Integer.parseInt((String)doc.getProperty("chunk_number"));
			for (int i = 0; i < cNo; ++i) {
				String cB64 = (String) doc.getProperty(String.valueOf(i));
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cBaos.write(Base64.decodeBase64(cB64));
				result.put(i, cBaos);
			}
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during read attempt from CouchDB! "
					+ "Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			BaseDocument plDoc = new BaseDocument();
			plDoc.setId(metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since")));
			plDoc.setProperty("payload_hash", metaInfo.get("payload_hash"));
			plDoc.setProperty("object_type", metaInfo.get("object_type"));
			plDoc.setProperty("version", metaInfo.get("version"));
			plDoc.setProperty("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.setProperty("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.setProperty("streamer_info", "streamer_info");
			plDoc.setProperty("chunk_number", String.valueOf(chunks.size()));
			for (int i = 0; i < chunks.size(); ++i) {
				String cB64 = Base64Util.encodeBase64(chunks.get(i).toByteArray());
				plDoc.setProperty(String.valueOf(i), cB64);
			}
			couchDB.createOrUpdateDocument(plDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}


}
