package hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HBaseBulkQueryHandler implements QueryHandler {

	private static HTable iovTable;
	private static HTable payloadTable;
	
	public HBaseBulkQueryHandler(String cluster) throws CustomSamplersException {
		iovTable = HBaseConfigElement.getHTable(cluster, "IOV");
		payloadTable = HBaseConfigElement.getHTable(cluster, "PAYLOAD");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		throw new CustomSamplersException("getData() method is not supported in the BulkQueryHandler!"
				+ " You should never see this exception...");
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		throw new CustomSamplersException("putData() method is not supported in the BulkQueryHandler!"
				+ " You should never see this exception...");
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();

		String compositeIOVKey = tagName.concat("_").concat(String.valueOf(since));
		Get iovGet = new Get(compositeIOVKey.getBytes());
		iovGet.addFamily("HASH".getBytes());

		List<Get> bulkGet = new ArrayList<Get>();
		try {
			Result iovR = iovTable.get(iovGet);
			NavigableMap<byte[], byte[]> hashColumnMap = iovR.getFamilyMap("HASH".getBytes());
			for(Entry<byte[], byte[]> columnEntry : hashColumnMap.entrySet())
			{
				Get chunkGet = new Get(columnEntry.getValue());
				bulkGet.add(chunkGet);
			}

			Result[] plR = payloadTable.get(bulkGet);
			for (int i = 0; i < plR.length; ++i) {
				Result chunkR = plR[i];
				NavigableMap<byte[], byte[]> chunkColumnMap = chunkR.getFamilyMap("DATA".getBytes());
				for(Entry<byte[], byte[]> chunkEntry : chunkColumnMap.entrySet())
				{
					ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
					cBaos.write(chunkEntry.getValue());
					result.put(ByteBuffer.wrap(chunkEntry.getKey()).getInt(), cBaos);
				}
				
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
		iovPut.add("META".getBytes(), "payload_hash".getBytes(), plHash.getBytes());
		iovPut.add("META".getBytes(), "version".getBytes(), metaInfo.get("version").getBytes());
		iovPut.add("META".getBytes(),
				"creation_time".getBytes(), Bytes.toBytes(System.currentTimeMillis()));
		iovPut.add("META".getBytes(),
				"cmssw_release".getBytes(), metaInfo.get("cmssw_release").getBytes());

		List<Put> bulkPut = new ArrayList<Put>();
		for (int i = 0; i < chunks.size(); ++i) {
			byte[] chunkHashBytes = metaInfo.get(String.valueOf(i+1)).getBytes();
			iovPut.add("HASH".getBytes(), Bytes.toBytes(i+1), chunkHashBytes);
			Put chunkPut = new Put(chunkHashBytes);
			chunkPut.add("DATA".getBytes(), Bytes.toBytes(i+1), chunks.get(i).toByteArray());
			bulkPut.add(chunkPut);
		}

		try {
			iovTable.put(iovPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into IOV HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}
		try {
			payloadTable.put(bulkPut);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured while writing into PAYLOAD HTable: "
					+ " MetaInfo: " + metaInfo.toString()
					+ "\n Exception details: " + e.toString());
		}
	}

}
