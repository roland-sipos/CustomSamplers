package riak;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.operations.StoreObject;

import utils.CustomSamplersException;
import utils.QueryHandler;

public class RiakQueryHandler implements QueryHandler {

	private static IRiakClient riakClient;

	/*protected class KryoBinaryPojoConverter implements Converter<Pojo> {
		private String bucket;

		public KryoBinaryPojoConverter(String bucket) {
			this.bucket = bucket;
		}

		@Override
		public IRiakObject fromDomain(BinaryPojo domainObject, VClock vclock) throws ConversionException {
			String key = getKey(domainObject);
			if (key == null)
				throw new NoKeySpecifedException(domainObject);
			Kryo kryo = new Kryo();
			kryo.register(type)
			return null;
		}

		@Override
		public BinaryPojo toDomain(IRiakObject arg0) throws ConversionException {
			// TODO Auto-generated method stub
			return null;
		}
	}*/

	public RiakQueryHandler(String clusterName) 
			throws CustomSamplersException {
		riakClient = RiakConfigElement.getRiakClient(clusterName);
		if (riakClient == null)
			throw new CustomSamplersException("IRiakClient instance with name: " + clusterName 
					+ " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			Bucket tB = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = tB.fetch(tagName + "_" + String.valueOf(since)).execute();
			Bucket pB = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject pObj = pB.fetch(tObj.getValueAsString()).execute();
			result.write(pObj.getValue());

		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured. Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException {
		try {
			// In the TAG bucket we store TAG_NAME + SINCE composite keys, value is a hash.
			Bucket tagB = riakClient.fetchBucket("TAG").execute();
			StoreObject<IRiakObject> sObj = tagB.store(
					metaInfo.get("tag_name") + "_" + metaInfo.get("since"),
					metaInfo.get("payload_hash"));
			// Also holds the PL's metadata, to avoid frequent fetching of binary body.
			IRiakObject obj = sObj.returnBody(true).execute();
			obj.addUsermeta("object_type", metaInfo.get("object_type"));
			obj.addUsermeta("streamer_info", "streamer_info");
			obj.addUsermeta("version", metaInfo.get("version"));
			obj.addUsermeta("creation_time", String.valueOf(System.currentTimeMillis()));
			obj.addUsermeta("cmssw_release", metaInfo.get("cmssw_release"));
			tagB.store(obj);

			// In payload, we have HASH -> Value pairs, with zero metadata.
			Bucket plB = riakClient.fetchBucket("PAYLOAD").execute();
			plB.store(metaInfo.get("payload_hash"), payload.toByteArray()).execute();

		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public Map<Integer, ByteArrayOutputStream> getChunks(String tagName, long since)
			throws CustomSamplersException {
		Map<Integer, ByteArrayOutputStream> result = new HashMap<Integer, ByteArrayOutputStream>();
		try {
			Bucket tB = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = tB.fetch(tagName + "_" + String.valueOf(since)).execute();
			Bucket pB = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject pObj = pB.fetch(tObj.getValueAsString()).execute();
			String[] hashes = pObj.getValueAsString().split("\\_");
			Bucket cBuck = riakClient.fetchBucket("CHUNK").execute();
			for (int j = 0; j < hashes.length; ++j) {
				ByteArrayOutputStream cS = new ByteArrayOutputStream();
				cS.write(cBuck.fetch(hashes[j]).execute().getValue());
				result.put(j+1, cS);
			}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured. Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			// In the TAG bucket we store TAG_NAME + SINCE composite keys, value is a hash.
			Bucket tagB = riakClient.fetchBucket("TAG").execute();
			StoreObject<IRiakObject> sObj = tagB.store(
					metaInfo.get("tag_name") + "_" + metaInfo.get("since"),
					metaInfo.get("payload_hash"));
			sObj.returnBody(false).execute();

			// Store chunks in advance, and create hash chain.
			String hashChain = "";
			Bucket cB = riakClient.fetchBucket("CHUNK").execute();
			for (int i = 0; i < chunks.size(); ++i) {
				String hash = metaInfo.get(String.valueOf(i+1));
				hashChain = hashChain.concat(hash).concat("_");
				cB.store(hash, chunks.get(i).toByteArray()).execute();
			}

			System.out.println(hashChain);
			// In payload, we have HASH -> Value pairs, with some metadata.
			Bucket plB = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject plObj = plB.store(metaInfo.get("payload_hash"), hashChain)
					.returnBody(true).execute();
			plObj.addUsermeta("object_type", metaInfo.get("object_type"));
			plObj.addUsermeta("streamer_info", "streamer_info");
			plObj.addUsermeta("version", metaInfo.get("version"));
			plObj.addUsermeta("creation_time", String.valueOf(System.currentTimeMillis()));
			plObj.addUsermeta("cmssw_release", metaInfo.get("cmssw_release"));
			plB.store(plObj).execute();

		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
		
	}


}
