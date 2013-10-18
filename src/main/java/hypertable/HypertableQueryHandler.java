package hypertable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HypertableQueryHandler implements QueryHandler {

	private static ThriftClient hyperTClient;
	private static Long hyperTNS = -1L;
	
	public HypertableQueryHandler(String cluster) throws CustomSamplersException {
		hyperTClient = HypertableConfigElement.getHypertableClient(cluster);
		hyperTNS = HypertableConfigElement.getHypertableNamespace(cluster);
	}

	private final Cell createCell(String rowKey, String columnFamily,
			String qualifier, byte[] value) {
		Key key = new Key();
		key.setRow(rowKey).setColumn_family(columnFamily);
		if (!qualifier.isEmpty()) {
			key.setColumn_qualifier(qualifier);
		}
		Cell cell = new Cell();
		cell.setKey(key);
		cell.setValue(value);
		return cell;
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		String iovRowKey = tagName.concat("_").concat(String.valueOf(since));

		try {
			ByteBuffer iovCell = hyperTClient.get_cell(hyperTNS, "IOV", iovRowKey, "HASH");
			//String plHash = new String(iovCell.array(), Charset.forName("UTF-8"));

			byte[] good = Arrays.copyOfRange(iovCell.array(), iovCell.array().length-41, iovCell.array().length);
			String plHash = new String(good, "UTF-8");
			System.out.println("Found hash: ".concat(plHash));

			ByteBuffer plCell = hyperTClient.get_cell(hyperTNS, "PAYLOAD", plHash, "DATA");

			System.out.println("Lenght of the array: " + plCell.array().length);
			result.write(plCell.array());

		} catch (ClientException e) {
			throw new CustomSamplersException("ClientException occured during write attempt!", e);
		} catch (TException e) {
			throw new CustomSamplersException("TException occured during write attempt!", e);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during write attempt!", e);
		}
		
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		String plHash = metaInfo.get("payload_hash");
		// Composite IOV Key: TAG_NAME + _ + SINCE
		String iovRowKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));

		try {
			// Flags: NO_LOG_SYNC (no sync()) and IGNORE_UNKNOWN_CFS
			long mutator = hyperTClient.mutator_open(hyperTNS, "IOV", 0, 0); // Flags, flush_interval
			hyperTClient.mutator_set_cell(mutator, 
					createCell(iovRowKey, "HASH", "", plHash.getBytes()) );
			hyperTClient.mutator_close(mutator);
			
			mutator = hyperTClient.mutator_open(hyperTNS, "PAYLOAD", 0, 0);
			hyperTClient.mutator_set_cell(mutator, 
					createCell(plHash, "DATA", "", payload.toByteArray()) );
			hyperTClient.mutator_close(mutator);

		} catch (ClientException e) {
			throw new CustomSamplersException("ClientException occured during write attempt!", e);
		} catch (TException e) {
			throw new CustomSamplersException("TException occured during write attempt!", e);
		}
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

}
