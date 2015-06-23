package accumulo;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class AccumuloQueryHandler implements QueryHandler {

	private static Connector accumulo;
	private static long bufferSize;
	private static long timeout;
	private static int numThreads;
	
	public AccumuloQueryHandler(String cluster, Long buffSize, Long tOut, Integer nThreads)
			throws CustomSamplersException {
		accumulo = AccumuloConfigElement.getAccumuloConnector(cluster);

		// These should be read by the Sampler!
		bufferSize = 1000000L; // bytes to store before sending a batch
		timeout = 1000L; // milliseconds to wait before sending
		numThreads = 10;
	}

	private final Mutation mutate(String rowKey, String colFamily, String colQualifier,
			String visibility, byte[] value) {
		Text rowId = new Text(rowKey);
		Text colF = new Text(colFamily);
		Text colQ = new Text(colQualifier);
		ColumnVisibility colV = new ColumnVisibility(visibility);

		Value v = new Value(value);
		Mutation mutation = new Mutation(rowId);
		// creationTime is always the creation time of the Mutation.
		mutation.put(colF, colQ, colV, System.currentTimeMillis(), v);
		return mutation;
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		String plHash = metaInfo.get("payload_hash");
		// Composite IOV Key: TAG_NAME + _ + SINCE
		String iovRowKey = metaInfo.get("tag_name").concat("_").concat(metaInfo.get("since"));
		Mutation iovRow = mutate(iovRowKey, "HASH", "", "public", plHash.getBytes());
		try {
			BatchWriter writer = accumulo.createBatchWriter("IOV", bufferSize, timeout, numThreads);
			writer.addMutation(iovRow);
			writer.close();
		} catch (TableNotFoundException e) {
			throw new CustomSamplersException("TableNotFoundException occured during write attempt!", e);
		} catch (MutationsRejectedException e) {
			throw new CustomSamplersException("MutationRejectedException occured during write attempt!", e);
		}
		
		// TODO: PAYLOAD NOT WRITTEN YET!
		
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName,
			long since) throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
