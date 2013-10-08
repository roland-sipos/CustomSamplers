package cassandra;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraBulkQueryHandler implements QueryHandler {

	private static Cluster cluster;
	private static Keyspace keyspace;
	private final static String iovCFName = "IOV";
	private final static String payloadCFName = "PAYLOAD";

	public CassandraBulkQueryHandler(String clusterName) 
			throws CustomSamplersException, NotFoundInDBException {
		cluster = CassandraConfigElement.getCassandraCluster(clusterName);
		KeyspaceDefinition ksDef = cluster.describeKeyspace("testKS");
		if (ksDef == null) {
			throw new NotFoundInDBException("Keyspace testKS not found in the "
					+ cluster.getName() + " cluster");
		}
		keyspace = HFactory.createKeyspace(ksDef.getName(), cluster);
		if (keyspace == null) {
			throw new NotFoundInDBException("Keyspace not found based on the definition: " + ksDef.getName());
		}
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		// NOT SUPPORTED: The framework will never reach this point.
		return null;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		// NOT SUPPORTED: The framework will never reach this point.
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			Composite key = new Composite();
			key.addComponent(metaInfo.get("tag_name"), StringSerializer.get());
			key.addComponent(metaInfo.get("since"), StringSerializer.get());

			Mutator<Composite> compMutator = HFactory.createMutator(keyspace, CompositeSerializer.get());
			Mutator<String> strMutator = HFactory.createMutator(keyspace, StringSerializer.get());
			for (int i = 0; i < chunks.size(); ++i) {
				String chunkHash = metaInfo.get(i+1);
				compMutator.addInsertion(key, iovCFName, HFactory.createColumn(i+1, chunkHash));
				// TODO: Not bulk insert, but: pl_hash -> (id:hash:data) -> (id:hash:data) etc ... 
				strMutator.addInsertion(chunkHash, payloadCFName,
						HFactory.createColumn("data", chunks.get(i).toByteArray()));
			}
			compMutator.execute();
			strMutator.execute();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}
	}

}
