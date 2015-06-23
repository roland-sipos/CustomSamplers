package hypertable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.MutateSpec;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HypertableQueryHandler implements QueryHandler {

	private volatile static ThriftClient hyperTClient;
	private volatile static Long hyperTNS = -1L;
	private volatile static MutateSpec mutateSpec;
	//private volatile static long plMutator;

	public HypertableQueryHandler(String cluster) throws CustomSamplersException {
		hyperTClient = HypertableConfigElement.getHypertableClient(cluster);
		hyperTNS = HypertableConfigElement.getHypertableNamespace(cluster);
		//try {
			mutateSpec = new MutateSpec();
			mutateSpec.setAppname("customsampler");
			mutateSpec.setFlush_interval(1000);
			/*iovMutator = hyperTClient.mutator_open(hyperTNS, "IOV", 0, 0);
			plMutator = hyperTClient.mutator_open(hyperTNS, "PAYLOAD", 0, 0);
			System.out.println("    -- iovMut: " + iovMutator);
			System.out.println("    --  plMut: " + plMutator);*/
		/*} catch (ClientException e) {
			throw new CustomSamplersException("ClientException occured while creation mutators!", e);
		} catch (TException e) {
			throw new CustomSamplersException("TException occured while creation mutators!", e);
		} // Flags, flush_interval*/
		System.out.println("   <- fetching: " + hyperTClient.toString() + " thread:" + Thread.currentThread().getName());
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		try {
			//hyperTClient.mutator_close(iovMutator);
			//hyperTClient.mutator_close(plMutator);
			mutateSpec.clear();
			hyperTClient.close();
			/*hyperTClient.mutator_flush(iovMutator);
			hyperTClient.mutator_close(iovMutator);
			hyperTClient.mutator_flush(plMutator);
			hyperTClient.mutator_close(plMutator);*/
		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured while closing the ThriftClient!", e);
		}
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
		cell.setValue(ByteBuffer.wrap(value));
		return cell;
	}

	@Override
	public synchronized ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		ByteBuffer result = null;
		String iovRowKey = tagName.concat("_").concat(String.valueOf(since));

		try {
			ByteBuffer iovCell = hyperTClient.get_cell(hyperTNS, "IOV", iovRowKey, "HASH");
			//String plHash = new String(iovCell.array(), Charset.forName("UTF-8"));

			byte[] good = Arrays.copyOfRange(iovCell.array(), iovCell.array().length-41, iovCell.array().length);
			String plHash = new String(good, "UTF-8");
			System.out.println("Found hash: ".concat(plHash));

			ByteBuffer plCell = hyperTClient.get_cell(hyperTNS, "PAYLOAD", plHash, "DATA");

			System.out.println("Lenght of the array: " + plCell.array().length);
			result = ByteBuffer.wrap(plCell.array());

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
	public synchronized void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		String plHash = metaInfo.get("payload_hash");
		// Composite IOV Key: TAG_NAME + _ + SINCE
		String iovRowKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));

		try {
			// Flags: NO_LOG_SYNC (no sync()) and IGNORE_UNKNOWN_CFS
			//long iovMutator = hyperTClient.mutator_open(hyperTNS, "IOV", 0, 0); // Flags, flush_interval
			/*hyperTClient.mutator_set_cell(iovMutator,
					createCell(iovRowKey, "HASH", "", plHash.getBytes()) );*/
			hyperTClient.shared_mutator_refresh(hyperTNS, "IOV", mutateSpec);
			hyperTClient.shared_mutator_set_cell(hyperTNS, "IOV", mutateSpec,
					createCell(iovRowKey, "HASH", "", plHash.getBytes()));
			//hyperTClient.mutator_flush(iovMutator);
			//hyperTClient.mutator_close(iovMutator);

			//long plMutator = hyperTClient.mutator_open(hyperTNS, "PAYLOAD", 0, 0);
			/*hyperTClient.mutator_set_cell(plMutator, 
					createCell(plHash, "DATA", "", payload.toByteArray()) );*/
			hyperTClient.shared_mutator_refresh(hyperTNS, "PAYLOAD", mutateSpec);
			hyperTClient.shared_mutator_set_cell(hyperTNS, "PAYLOAD", mutateSpec,
					createCell(plHash, "DATA", "", payload.toByteArray()) );
			//hyperTClient.mutator_flush(plMutator);
			//hyperTClient.mutator_close(plMutator);

		} catch (ClientException e) {
			throw new CustomSamplersException("ClientException occured during write attempt!", e);
		} catch (TException e) {
			throw new CustomSamplersException("TException occured during write attempt!", e);
		}
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName,
			long since) throws CustomSamplersException {
		return null;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		String plHash = metaInfo.get("payload_hash");
		// Composite IOV Key: TAG_NAME + _ + SINCE
		String iovRowKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
		System.out.println(" -> CompKey: " + iovRowKey + " rk:" + plHash);
		/*try {
			// Flags: NO_LOG_SYNC (no sync()) and IGNORE_UNKNOWN_CFS
			//long iovMutator = hyperTClient.mutator_open(hyperTNS, "IOV", 0, 0); // Flags, flush_interval
			hyperTClient.mutator_set_cell(iovMutator, 
					createCell(iovRowKey, "HASH", "", plHash.getBytes()) );
			//hyperTClient.mutator_flush(iovMutator);
			//hyperTClient.mutator_close(iovMutator);

			//long plMutator = hyperTClient.mutator_open(hyperTNS, "PAYLOAD", 0, 0);
			hyperTClient.mutator_set_cells(plMutator, 
					createCell(plHash, "DATA", "", payload.toByteArray()) );
			//hyperTClient.mutator_flush(plMutator);
			//hyperTClient.mutator_close(plMutator);

		} catch (ClientException e) {
			throw new CustomSamplersException("ClientException occured during write attempt!", e);
		} catch (TException e) {
			throw new CustomSamplersException("TException occured during write attempt!", e);
		}*/

	}

}
