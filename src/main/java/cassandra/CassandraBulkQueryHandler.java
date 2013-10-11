package cassandra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.CustomSamplersException;
import utils.QueryHandler;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraBulkQueryHandler implements QueryHandler {

	private static Cluster cluster;
	private static Keyspace keyspace;
	private final static String iovCFName = "IOV";
	private final static String payloadCFName = "PAYLOAD";

	public CassandraBulkQueryHandler(String clusterName) 
			throws CustomSamplersException {
		cluster = CassandraConfigElement.getCassandraCluster(clusterName);
		KeyspaceDefinition ksDef = cluster.describeKeyspace("testKS");
		if (ksDef == null) {
			throw new CustomSamplersException("Keyspace testKS not found in the "
					+ cluster.getName() + " cluster!");
		}
		keyspace = HFactory.createKeyspace(ksDef.getName(), cluster);
		if (keyspace == null) {
			throw new CustomSamplersException("Keyspace not found based on the definition: "
					+ ksDef.getName());
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
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			StringSerializer ss = StringSerializer.get();

			Composite key = new Composite();
			key.addComponent(tagName, ss);
			key.addComponent(String.valueOf(since), ss);
			
			SliceQuery<Composite, Integer, String> iovQuery = HFactory
					.createSliceQuery(keyspace, CompositeSerializer.get(), IntegerSerializer.get(), ss)
					.setColumnFamily(iovCFName)
					.setKey(key);

			ColumnSliceIterator<Composite, Integer, String> iterator = 
					new ColumnSliceIterator<Composite, Integer, String>(
							iovQuery, 0, 10000, false);

			ColumnQuery<String, Integer, byte[]> plQuery = HFactory
					.createColumnQuery(keyspace, ss, IntegerSerializer.get(), BytesArraySerializer.get())
					.setColumnFamily(payloadCFName);
			while (iterator.hasNext()) {
				HColumn<Integer, String> hashColumn = iterator.next();
				plQuery.setKey(hashColumn.getValue()).setName(hashColumn.getName());
				QueryResult<HColumn<Integer, byte[]> > cResult = plQuery.execute();
				ByteArrayOutputStream cBaos = new ByteArrayOutputStream();
				cBaos.write(cResult.get().getValue());
				result.put(Integer.valueOf(hashColumn.getName()), cBaos);
			}
		} catch (HectorException he) {
			throw new CustomSamplersException("HectorException occured during write attempt -> ", he);
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during write attempt -> ", e);
		}
		return result;
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
				String chunkHash = metaInfo.get(String.valueOf(i+1));
				compMutator.addInsertion(key, iovCFName, HFactory.createColumn(i+1, chunkHash));
				// TODO: Not bulk insert, but: pl_hash -> (id:hash:data) -> (id:hash:data) etc ... 
				strMutator.addInsertion(chunkHash, payloadCFName,
						HFactory.createColumn(i+1, chunks.get(i).toByteArray()));
			}
			compMutator.execute();
			strMutator.execute();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured -> ", he);
		}
	}

}
