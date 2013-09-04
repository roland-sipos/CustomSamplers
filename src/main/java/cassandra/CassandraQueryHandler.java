package cassandra;

import java.util.HashMap;

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
	private static String columnFamilyName;
	
	public CassandraQueryHandler(String clusterName, String keyspaceName, String cfName) 
			throws CustomSamplersException, NotFoundInDBException {
		cluster = CassandraConfigElement.getCassandraCluster(clusterName);
		KeyspaceDefinition ksDef = cluster.describeKeyspace(keyspaceName);
		if (ksDef == null)
			throw new NotFoundInDBException("Keyspace not found in the " + cluster.getName() + " cluster");
		keyspace = HFactory.createKeyspace(ksDef.getName(), cluster);
		if (keyspace == null)
			throw new NotFoundInDBException("Keyspace not found based on the definition: " + ksDef.getName());
		columnFamilyName = cfName;
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

	@Override
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

	@Override
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
	public void writePayload(HashMap<String, String> metaMap, byte[] payload,
			byte[] streamerInfo, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readPayload(HashMap<String, String> metaMap, boolean isSpecial)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
