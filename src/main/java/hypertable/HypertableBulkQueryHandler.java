package hypertable;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hypertable.thrift.ThriftClient;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class HypertableBulkQueryHandler implements QueryHandler {

	private static ThriftClient hyperTClient;
	private static Long hyperTNS = -1L;

	public HypertableBulkQueryHandler(String cluster) throws CustomSamplersException {
		hyperTClient = HypertableConfigElement.getHypertableClient(cluster);
		hyperTNS = HypertableConfigElement.getHypertableNamespace(cluster);
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
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

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

}
