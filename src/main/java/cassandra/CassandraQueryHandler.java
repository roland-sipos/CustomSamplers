package cassandra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
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

public class CassandraQueryHandler implements QueryHandler {

	private static Cluster cluster;
	private static Keyspace keyspace;
	private final static String iovCFName = "IOV";
	private final static String payloadCFName = "PAYLOAD";

	public CassandraQueryHandler(String clusterName) 
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

	public byte[] readBinaryFromCassandra(String columnFamilyName, String columnName, 
			String originalHash, String chunkHash)
					throws NotFoundInDBException, CustomSamplersException {
		Composite key = new Composite();
		key.addComponent(originalHash, StringSerializer.get());
		key.addComponent(chunkHash, StringSerializer.get());
		byte[] value = null;
		try {
			ColumnQuery<Composite, String, byte[]> columnQuery = 
					HFactory.createColumnQuery(keyspace, CompositeSerializer.get(), 
							StringSerializer.get(), 
							BytesArraySerializer.get());
			columnQuery.setColumnFamily(columnFamilyName).setKey(key).setName(columnName);
			QueryResult<HColumn<String, byte[]> > result = columnQuery.execute();
			value = result.get().getValue();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}

		if (value == null)
			throw new NotFoundInDBException("Row with the key not found in the database");

		return value;

	}

	public void writeBinaryToCassandra(String columnFamilyName, String columnName,
			String originalHash, String chunkHash, byte[] value) 
					throws CustomSamplersException {
		try {
			Composite key = new Composite();
			key.addComponent(originalHash, StringSerializer.get());
			key.addComponent(chunkHash, StringSerializer.get());
			Mutator<Composite> compMutator = HFactory.createMutator(keyspace, CompositeSerializer.get());
			compMutator.addInsertion(key, columnFamilyName, HFactory.createColumn(columnName, value));
			compMutator.execute();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}
	}

	//@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial)
					throws CustomSamplersException {
		try {
			//Composite key = new Composite();
			//key.addComponent(originalHash, StringSerializer.get());
			//key.addComponent(chunkHash, StringSerializer.get());
			String key = hash;
			Mutator<String> strMutator = HFactory.createMutator(keyspace, StringSerializer.get());
			strMutator.addInsertion(key, columnFamilyName, HFactory.createColumn(chunkID, fileContent));
			strMutator.execute();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}

	}

	//@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash,
			boolean isSpecial) throws CustomSamplersException {
		//Composite key = new Composite();
		//key.addComponent(originalHash, StringSerializer.get());
		//key.addComponent(chunkHash, StringSerializer.get());
		byte[] value = null;
		try {
			ColumnQuery<String, String, byte[]> columnQuery = 
					HFactory.createColumnQuery(keyspace, StringSerializer.get(), 
							StringSerializer.get(), 
							BytesArraySerializer.get());
			columnQuery.setColumnFamily(columnFamilyName).setKey(hash).setName(chunkID);
			QueryResult<HColumn<String, byte[]> > result = columnQuery.execute();
			value = result.get().getValue();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}

		if (value == null)
			throw new CustomSamplersException("Row with the key not found in the database");

		return value;
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			StringSerializer ss = StringSerializer.get();

			Composite key = new Composite();
			key.addComponent(tagName, ss);
			key.addComponent(String.valueOf(since), ss);

			ColumnQuery<Composite, String, String> iovQuery =
					HFactory.createColumnQuery(keyspace, CompositeSerializer.get(), ss, ss);
			iovQuery.setColumnFamily(iovCFName).setKey(key).setName("hash");
			QueryResult<HColumn<String, String> > iovResult = iovQuery.execute();
			String hash = iovResult.get().getValue();
			
			ColumnQuery<String, String, byte[]> plQuery = 
					HFactory.createColumnQuery(keyspace, ss, ss, BytesArraySerializer.get());
			plQuery.setColumnFamily(payloadCFName).setKey(hash).setName("data");
			QueryResult<HColumn<String, byte[]> > plResult = plQuery.execute();
			result.write(plResult.get().getValue());
		} catch (HectorException he) {
			throw new CustomSamplersException("HectorException occured during write attempt:" + he.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during write attempt:" + e.toString());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) throws CustomSamplersException {
		try {
			Composite key = new Composite();
			key.addComponent(metaInfo.get("tag_name"), StringSerializer.get());
			key.addComponent(metaInfo.get("since"), StringSerializer.get());

			String hash = metaInfo.get("payload_hash");
			Mutator<Composite> compMutator = HFactory.createMutator(keyspace, CompositeSerializer.get());
			compMutator.addInsertion(key, iovCFName, HFactory.createColumn("hash", hash));
			compMutator.execute();

			Mutator<String> strMutator = HFactory.createMutator(keyspace, StringSerializer.get());
			strMutator.addInsertion(hash, payloadCFName,
					HFactory.createColumn("data", payload.toByteArray()));
			strMutator.execute();
		} catch (HectorException he) {
			throw new CustomSamplersException("Hector exception occured:" + he.toString());
		}
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
		// TODO Auto-generated method stub

	}

}
