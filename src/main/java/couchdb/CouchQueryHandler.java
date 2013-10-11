package couchdb;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.commons.codec.binary.Base64;
import org.ektorp.CouchDbConnector;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class CouchQueryHandler implements QueryHandler {

	private static CouchDbConnector couchDB;

	public CouchQueryHandler(String databaseName) 
			throws CustomSamplersException {
		couchDB = CouchConfigElement.getCouchDB(databaseName);
		if (couchDB == null)
			throw new CustomSamplersException("CouchDB Database instance with name: " 
					+ databaseName + " was not found in config!");
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc = couchDB.get(Map.class, tagName.concat("_").concat(String.valueOf(since)));
			result.write((byte[])plDoc.get("data"));
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
			Map<String, Object> plDoc = new HashMap<String, Object>();
			//BaseDocument plDoc = new BaseDocument();
			//plDoc.put("_id", metaInfo.get("tag_name"));
			plDoc.put("_id", metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since")));
			//plDoc.put("_rev", metaInfo.get("since"));
			plDoc.put("payload_hash", metaInfo.get("payload_hash"));
			plDoc.put("object_type", metaInfo.get("object_type"));
			plDoc.put("version", metaInfo.get("version"));
			plDoc.put("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.put("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.put("streamer_info", "streamer_info");
			//String valueBase64 = Base64Util.encodeBase64(payload.toByteArray());
			plDoc.put("data", payload.toByteArray());
			couchDB.create(plDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc = couchDB.get(Map.class, tagName.concat("_").concat(String.valueOf(since)));
			int cNo = Integer.parseInt((String)plDoc.get("chunk_number"));
			for (int i = 0; i < cNo; ++i) {
				//String cB64 = (String) doc.getProperty(String.valueOf(i));
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cBaos.write((byte[])plDoc.get(String.valueOf(i)));
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
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc.put("_id", metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since")));
			//plDoc.put("_rev", metaInfo.get("since"));
			plDoc.put("payload_hash", metaInfo.get("payload_hash"));
			plDoc.put("object_type", metaInfo.get("object_type"));
			plDoc.put("version", metaInfo.get("version"));
			plDoc.put("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.put("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.put("streamer_info", "streamer_info");
			plDoc.put("chunk_number", String.valueOf(chunks.size()));
			for (int i = 0; i < chunks.size(); ++i) {
				//String cB64 = Base64Util.encodeBase64(chunks.get(i).toByteArray());
				plDoc.put(String.valueOf(i), chunks.get(i).toByteArray());
			}
			couchDB.create(plDoc);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}


}
