package couchdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ektorp.AttachmentInputStream;
import org.ektorp.CouchDbConnector;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class CouchAttachmentQueryHandler implements QueryHandler {

	private static CouchDbConnector couchDB;
	//private static SampleResult sampleResult;

	public CouchAttachmentQueryHandler(String databaseName) 
			throws CustomSamplersException {
		couchDB = CouchConfigElement.getCouchDB(databaseName);
		//sampleResult = res;
		if (couchDB == null)
			throw new CustomSamplersException("CouchDB Database instance with name: " 
					+ databaseName + " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			String id = tagName.concat("_").concat(String.valueOf(since));
			//sampleResult.samplePause();
			AttachmentInputStream dataIS = couchDB.getAttachment(id, "data");//, couchDB.getCurrentRevision(id));
			//sampleResult.sampleResume();
			int size = (int)dataIS.getContentLength();
			byte[] data = new byte[size];
			int nRead = 0;
			while ((nRead = dataIS.read(data, 0, size)) != -1) {
				result.write(data, 0, nRead);
			}
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
			String id = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc.put("_id", id);
			plDoc.put("payload_hash", metaInfo.get("payload_hash"));
			plDoc.put("object_type", metaInfo.get("object_type"));
			plDoc.put("version", metaInfo.get("version"));
			plDoc.put("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.put("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.put("streamer_info", "streamer_info");
			couchDB.create(plDoc);

			InputStream dataS = new ByteArrayInputStream(payload.toByteArray());
			AttachmentInputStream attS =
					new AttachmentInputStream("data", dataS, "application/octet-stream");
			couchDB.createAttachment(id, couchDB.getCurrentRevision(id), attS);

		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			String id = tagName.concat("_").concat(String.valueOf(since));
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc = couchDB.get(Map.class, id);
			int cNo = Integer.parseInt((String)plDoc.get("chunk_number"));
			for (int i = 0; i < cNo; ++i) {
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				AttachmentInputStream dataIS = couchDB.getAttachment(id, String.valueOf(i+1));
				int size = (int)dataIS.getContentLength();
				byte[] data = new byte[size];
				int nRead = 0;
				while ((nRead = dataIS.read(data, 0, size)) != -1) {
					cBaos.write(data, 0, nRead);
				}
				result.put(i+1, cBaos);
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
			String id = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
			Map<String, Object> plDoc = new HashMap<String, Object>();
			plDoc.put("_id", id);
			plDoc.put("payload_hash", metaInfo.get("payload_hash"));
			plDoc.put("object_type", metaInfo.get("object_type"));
			plDoc.put("version", metaInfo.get("version"));
			plDoc.put("cmssw_release", metaInfo.get("cmssw_release"));
			plDoc.put("creation_time", String.valueOf(System.currentTimeMillis()));
			plDoc.put("streamer_info", "streamer_info");
			plDoc.put("chunk_number", String.valueOf(chunks.size()));
			couchDB.create(plDoc);

			String rev = couchDB.getCurrentRevision(id);
			for (int i = 0; i < chunks.size(); ++i) {
				InputStream chunkIS = new ByteArrayInputStream(chunks.get(i).toByteArray());
				AttachmentInputStream chunkAIS = new AttachmentInputStream(
						String.valueOf(i+1), chunkIS, "application/octet-stream");
				rev = couchDB.createAttachment(id, rev, chunkAIS);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new CustomSamplersException("Exception occured during write attempt to CouchDB! "
					+ "Details: " + e.toString());
		}
	}

}
