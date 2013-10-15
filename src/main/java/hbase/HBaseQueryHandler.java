package hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HBaseQueryHandler implements QueryHandler {

	//private static Configuration hbaseConfig;
	private static HTable iovTable;
	private static HTable payloadTable;

	public HBaseQueryHandler(String cluster) throws CustomSamplersException {
		//hbaseConfig = HBaseConfigElement.getHBaseConfiguration(cluster);
		iovTable = HBaseConfigElement.getHTable(cluster, "IOV");
		payloadTable = HBaseConfigElement.getHTable(cluster, "PAYLOAD");
	}

	public void tearDown() throws CustomSamplersException {
		try {
			iovTable.close();
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while closing the IOV HTable: "
					+ " Details: " + e.toString());
		}
		try {
			payloadTable.close();
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while closing the PAYLOAD HTable: "
					+ " Details: " + e.toString());
		}
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();

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
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException {
		String compositeIOVKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
		String hash = metaInfo.get("payload_hash");
		Put iovPut = new Put(compositeIOVKey.getBytes());
		iovPut.add("HASH".getBytes(), "payload_hash".getBytes(), hash.getBytes());
		try {
			iovTable.put(iovPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into IOV HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}

		Put payloadPut = new Put(Bytes.toBytes(hash));
		payloadPut.add("META".getBytes(),
				"version".getBytes(), metaInfo.get("version").getBytes());
		payloadPut.add("META".getBytes(),
				"creation_time".getBytes(), Bytes.toBytes(System.currentTimeMillis()));
		payloadPut.add("META".getBytes(),
				"cmssw_release".getBytes(), metaInfo.get("cmssw_release").getBytes());

		payloadPut.add("DATA".getBytes(),
				"payload".getBytes(), payload.toByteArray());
		try {
			payloadTable.put(payloadPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into PAYLOAD HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}

	}

	private Integer fromByteArray(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();

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

		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {

		String plHash = metaInfo.get("payload_hash");
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
		}
	}

}

