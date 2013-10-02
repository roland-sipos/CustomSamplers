package couchdb;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jcouchdb.db.Database;
import org.jcouchdb.document.BaseDocument;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class CouchAttachmentQueryHandler implements QueryHandler {

	private static Database couchDB;

	public CouchAttachmentQueryHandler(String databaseName) 
			throws CustomSamplersException, NotFoundInDBException {
		couchDB = CouchConfigElement.getCouchDB(databaseName);
		if (couchDB == null)
			throw new NotFoundInDBException("CouchDB Database instance with name: " 
					+ databaseName + " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			BaseDocument doc = couchDB.getDocument(BaseDocument.class,
					tagName.concat("_").concat(String.valueOf(since)));
			result.write(couchDB.getAttachment(doc.getId(), "data"));
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during read attempt from CouchDB! "
					+ "Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException {
		try {
			BaseDocument plDoc = new BaseDocument();
			plDoc.setId(metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since")));
			plDoc.setProperty("payload_hash", metaInfo.get("payload_hash"));
			plDoc.setProperty("object_type", metaInfo.get("object_type"));
			plDoc.setProperty("version", metaInfo.get("version"));
			plDoc.setProperty("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.setProperty("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.setProperty("streamer_info", "streamer_info");
			couchDB.createOrUpdateDocument(plDoc);
			couchDB.createAttachment(plDoc.getId(), plDoc.getRevision(), 
					"data", "application/octet-stream", payload.toByteArray());
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			BaseDocument doc = couchDB.getDocument(BaseDocument.class,
					tagName.concat("_").concat(String.valueOf(since)));
			int cNo = Integer.parseInt((String)doc.getProperty("chunk_number"));
			for (int i = 0; i < cNo; ++i) {
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cBaos.write(couchDB.getAttachment(doc.getId(), String.valueOf(i)));
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
			couchDB.createOrUpdateDocument(plDoc);
			for (int i = 0; i < chunks.size(); ++i) {
				System.out.println(" -> Adding " + i + ". chunk...");
				//plDoc.addAttachment(String.valueOf(i), 
					//	new Attachment("application/octet-stream", chunks.get(i).toByteArray()));
				couchDB.updateAttachment(plDoc.getId(), plDoc.getRevision(),
						String.valueOf(i), "application/octet-stream", chunks.get(i).toByteArray());
			}
			plDoc = null;
			System.out.println(couchDB.getStatus().getDocumentCount());
		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

}
