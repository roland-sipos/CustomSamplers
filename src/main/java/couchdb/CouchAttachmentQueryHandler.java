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

/**
 * This is the implemented QueryHandler for CouchDB databases, using the Attachments API.
 * */
public class CouchAttachmentQueryHandler implements QueryHandler {

	/** The CouchDB Connection object, fetched from a CouchConfigElement. */
	private static CouchDbConnector couchDB;

	public CouchAttachmentQueryHandler(String connectionId) 
			throws CustomSamplersException {
		couchDB = CouchConfigElement.getCouchDB(connectionId);
		//sampleResult = res;
		if (couchDB == null)
			throw new CustomSamplersException("CouchDB Database instance with name: " 
					+ connectionId + " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			String id = tagName.concat("_").concat(String.valueOf(since));
			String rev = couchDB.getRevisions(id).get(0).getRev();
			AttachmentInputStream dataIS = couchDB.getAttachment(id, "data", rev);
			int size = (int) dataIS.getContentLength();
			byte[] data = new byte[size];
			int nRead = 0;
			while ((nRead = dataIS.read(data, 0, size)) != -1) {
				result.write(data, 0, nRead);
			}

			/** NOTE: http://ektorp.org/javadoc/ektorp/1.4.1/ CouchDbConnector.getAttachment():
			 * Please note that the stream has to be closed after usage, otherwise http connection 
			 * leaks will occur and the system will eventually hang due to connection starvation.
			 * */
			dataIS.close();
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
			attS.close();
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

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
