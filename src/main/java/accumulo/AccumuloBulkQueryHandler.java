package accumulo;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class AccumuloBulkQueryHandler implements QueryHandler {

	private static Connector accumulo;

	public AccumuloBulkQueryHandler(String cluster, Long buffSize, Long tOut, Integer nThreads)
			throws CustomSamplersException {
		accumulo = AccumuloConfigElement.getAccumuloConnector(cluster);
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
			throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName,
			long since) throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

}
