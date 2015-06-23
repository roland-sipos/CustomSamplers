package hbase;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HBaseQueryHandler implements QueryHandler {

	private static HBaseClient hbaseClient;

	public HBaseQueryHandler(String connectionId) throws CustomSamplersException {
		//hbaseConfig = HBaseConfigElement.getHBaseConfiguration(cluster);
		//iovTable = HBaseConfigElement.getHTable(cluster, "IOV");
		//payloadTable = HBaseConfigElement.getHTable(cluster, "PAYLOAD");
		hbaseClient = HBaseConfigElement.getHBaseClient(connectionId);
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		String iovKey = tagName.concat("_").concat(String.valueOf(since));
		try {
			GetRequest iovGet = new GetRequest("IOV", iovKey.getBytes());
			ArrayList<KeyValue> hashes = hbaseClient.get(iovGet).join();
			//if (hashes.size() > 1) {
			//	throw new CustomSamplersException("More than one payload hash found for key: " + iovKey);
			//}

			GetRequest plGet = new GetRequest("PAYLOAD", hashes.get(0).value());
			ArrayList<KeyValue> payloads = hbaseClient.get(plGet).join();
			return ByteBuffer.wrap(payloads.get(0).value());
		} catch (InterruptedException e) {
			throw new CustomSamplersException("InterruptedException during HBase getData.", e);
		} catch (Exception e) {
			throw new CustomSamplersException("Exception during HBase getData.", e);
		}
		
		/*ByteArrayOutputStream result = new ByteArrayOutputStream();

		String compositeIOVKey = tagName.concat("_").concat(String.valueOf(since));
		Get iovGet = new Get(compositeIOVKey.getBytes());
		iovGet.addColumn("HASH".getBytes(), "payload_hash".getBytes());

		byte[] plHash = null;
		try {
			Result rr = iovTable.get(iovGet);
			plHash = rr.getValue("HASH".getBytes(), "payload_hash".getBytes());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured on Get request in IOV HTable: "
					+ " Get:" + iovGet.toString()
					+ " Details: " + e.toString());
		}

		Get plGet = new Get(plHash);
		plGet.addColumn("DATA".getBytes(), "payload".getBytes());
		try {
			Result rr = payloadTable.get(plGet);
			result.write(
					rr.getValue("DATA".getBytes(), "payload".getBytes()));
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured on Get request in Payload HTable: "
					+ " Get:" + plGet.toString()
					+ " Details: " + e.toString());
		}
		return result;*/
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException {
		try {
			String compIOVKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
			String plHash = metaInfo.get("payload_hash");
			PutRequest iovPut = new PutRequest("IOV".getBytes(), compIOVKey.getBytes(),
					"HASH".getBytes(), "hash".getBytes(), plHash.getBytes());
			hbaseClient.put(iovPut).join();

			ByteBuffer buff = ByteBuffer.wrap(payload.toByteArray());
			PutRequest plPut = new PutRequest("PAYLOAD".getBytes(), plHash.getBytes(),
					"DATA".getBytes(), "data".getBytes(), buff.array());
			hbaseClient.put(plPut).join();
			buff.clear();
		} catch (InterruptedException ie) {
			throw new CustomSamplersException("InterruptedException occured during PAYLOAD PUT: "
					+ ie.toString() +  " Message: " + ie.getMessage());
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during PAYLOAD PUT: "
					+ e.toString() +  " Message: " + e.getMessage());
		}
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException {
		throw new CustomSamplersException("GetChunks with HBaseAsync is not yet supported...");
		/*Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();

		String compositeIOVKey = tagName.concat("_").concat(String.valueOf(since));
		Get iovGet = new Get(compositeIOVKey.getBytes());
		iovGet.addFamily("HASH".getBytes());

		try {
			Result iovR = iovTable.get(iovGet);
			byte[] plHashBytes = iovR.getValue("META".getBytes(), "payload_hash".getBytes());

			Get plGet = new Get(plHashBytes);
			plGet.addFamily("DATA".getBytes());
			plGet.addFamily("META".getBytes());
			Result plR = payloadTable.get(plGet);
			NavigableMap<byte[], byte[]> columnMap = plR.getFamilyMap("DATA".getBytes());
			for(Entry<byte[], byte[]> columnEntry : columnMap.entrySet())
			{
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cBaos.write(columnEntry.getValue());

				result.put(fromByteArray(columnEntry.getKey()), cBaos);
			}

		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured on Get request in IOV HTable: "
					+ " Get:" + iovGet.toString()
					+ " Details: " + e.toString());
		}

		return result;*/
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		throw new CustomSamplersException("PutChunks with AsyncHbase is not yet supported...");
		/*String plHash = metaInfo.get("payload_hash");
		String compositeIOVKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
		Put iovPut = new Put(compositeIOVKey.getBytes());
		iovPut.add("HASH".getBytes(), "payload_hash".getBytes(), plHash.getBytes());

		Put payloadPut = new Put(Bytes.toBytes(plHash));
		payloadPut.add("META".getBytes(),
				"version".getBytes(), metaInfo.get("version").getBytes());
		payloadPut.add("META".getBytes(),
				"creation_time".getBytes(), Bytes.toBytes(System.currentTimeMillis()));
		payloadPut.add("META".getBytes(),
				"cmssw_release".getBytes(), metaInfo.get("cmssw_release").getBytes());
		for (int i = 0; i < chunks.size(); ++i) {
			byte[] chunkHashBytes = metaInfo.get(String.valueOf(i+1)).getBytes();
			payloadPut.add("DATA".getBytes(), Bytes.toBytes(i+1), chunks.get(i).toByteArray());
			payloadPut.add("META".getBytes(), Bytes.toBytes(i+1), chunkHashBytes);
		}

		try {
			iovTable.put(iovPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into IOV HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}
		try {
			payloadTable.put(payloadPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into PAYLOAD HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}*/
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}

