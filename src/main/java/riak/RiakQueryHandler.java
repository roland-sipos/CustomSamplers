package riak;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
//import com.basho.riak.client.api.commands.kv.StoreValue.Option;
//import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

/*import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.query.MultiFetchFuture;*/

import utils.CustomSamplersException;
import utils.QueryHandler;

public class RiakQueryHandler implements QueryHandler {

	private static RiakClient riakClient;
	private static Namespace nsIOV;
	private static Namespace nsPayload;
	//private static Bucket iovBucket;
	//private static Bucket payloadBucket;
	//private static Bucket chunkBucket;

	public RiakQueryHandler(String connectionId) 
			throws CustomSamplersException {
		riakClient = RiakConfigElement.getRiakClient(connectionId);
		nsIOV = new Namespace("default","IOV");
		nsPayload = new Namespace("default", "PAYLOAD");
		//iovBucket = RiakConfigElement.getBucket(connectionId.concat("-IOV"));
		//payloadBucket = RiakConfigElement.getBucket(connectionId.concat("-PAYLOAD"));
		//chunkBucket = RiakConfigElement.getBucket(connectionId.concat("-CHUNK"));
		if (riakClient == null)
			throw new CustomSamplersException("IRiakClient instance with name: " + connectionId 
					+ " was not found in config!");
	}

	@Override
	public ByteBuffer getData(String tagName, long since)
			throws CustomSamplersException {
		try {
			FetchValue fvHash = new FetchValue.Builder(
					new Location(nsIOV, tagName + "_" + String.valueOf(since))).build();
			String hash  = riakClient.execute(fvHash).getValue(String.class);

			FetchValue fvPL = new FetchValue.Builder(new Location(nsPayload, hash))
				.withOption(FetchValue.Option.BASIC_QUORUM, false)
				.withOption(FetchValue.Option.N_VAL, 1)
				//.withOption(FetchValue.Option.PR, Quorum.oneQuorum())
				.withOption(FetchValue.Option.R, Quorum.oneQuorum())
				.build();
			RiakObject plObj = riakClient.execute(fvPL).getValue(RiakObject.class);
			return ByteBuffer.wrap(plObj.getValue().getValue());

			/*IRiakObject iovObj = iovBucket.fetch(tagName + "_" + String.valueOf(since)).execute();
			String hash = iovObj.getValueAsString();
			IRiakObject plObj = payloadBucket.fetch(hash).r(Quora.ONE).execute();
			return ByteBuffer.wrap(plObj.getValue());*/

		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured for RIAK read.", e);
		}
		/*} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}*/
	}

	@Override
	public void putData(HashMap<String, String> metaInfo,
			ByteArrayOutputStream payload, ByteArrayOutputStream streamerInfo)
					throws CustomSamplersException {
		try {
			//Namespace ns = new Namespace("default", "my_bucket");
			Location iovKey = new Location(nsIOV,
					metaInfo.get("tag_name") + "_" + metaInfo.get("since"));
			StoreValue storeIov = new StoreValue.Builder(metaInfo.get("payload_hash"))
				.withLocation(iovKey)
				.withOption(StoreValue.Option.W, Quorum.allQuorum()).build();
			riakClient.execute(storeIov);

			Location plKey = new Location(nsPayload, metaInfo.get("payload_hash"));
			RiakObject riakPayload = new RiakObject();
			riakPayload.setValue(BinaryValue.create(payload.toByteArray()));
			StoreValue storePl = new StoreValue.Builder(riakPayload)
				.withLocation(plKey).build();
			riakClient.execute(storePl);

			/*iovBucket.store(
					metaInfo.get("tag_name") + "_" + metaInfo.get("since"),
					metaInfo.get("payload_hash")).execute();

			// In payload, we have HASH -> Value pairs, with zero metadata.
			Bucket plB = riakClient.fetchBucket("PAYLOAD").execute();
			plB.store(metaInfo.get("payload_hash"), payload.toByteArray()).execute();*/

		} catch (Exception e) {
			throw new CustomSamplersException("Exception occured during RIAK write.", e);
		}
		/*} catch (RiakRetryFailedException e) {
			e.printStackTrace();
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}*/
	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since)
			throws CustomSamplersException {
		TreeMap<Integer, ByteBuffer> result = new TreeMap<Integer, ByteBuffer>();
		/*try {
			IRiakObject iovObj = iovBucket.fetch(tagName + "_" + String.valueOf(since)).execute();
			List<MultiFetchFuture<IRiakObject> > chunks =
					payloadBucket.multiFetch(iovObj.getValueAsString().split("_")).execute();
			int idx = 1;
			for (MultiFetchFuture<IRiakObject> c : chunks) {
				result.put(idx, ByteBuffer.wrap(c.get().getValue()));
			}

			//for (int i = 0; i < hashes.length; ++i) {
			//	result.put(i+1, ByteBuffer.wrap(payloadBucket.fet)
			//}
			//IRiakObject plObj = payloadBucket.fetch(hash).execute();
			
			//Bucket tB = riakClient.fetchBucket("TAG").execute();
			//IRiakObject tObj = tB.fetch(tagName + "_" + String.valueOf(since)).execute();
			//Bucket pB = riakClient.fetchBucket("PAYLOAD").execute();
			//IRiakObject pObj = pB.fetch(tObj.getValueAsString()).execute();
			//String[] hashes = pObj.getValueAsString().split("\\_");
			//Bucket cBuck = riakClient.fetchBucket("CHUNK").execute();
			//for (int j = 0; j < hashes.length; ++j) {
			//	result.put(j+1, ByteBuffer.wrap(
			//			cBuck.fetch(hashes[j]).execute().getValue())
			//	);
			//}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (InterruptedException e) {
			throw new CustomSamplersException("InterruptedException occured. Details: " + e.toString());
		} catch (ExecutionException e) {
			throw new CustomSamplersException("ExecutionException occured. Details: " + e.toString());
		}*/
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		/*try {
			//List<String> chunkHashes = new ArrayList<String>();
			String chunkHashes = "";
			for (int i = 0; i < chunks.size(); ++i) { 
				chunkHashes.concat(String.valueOf(i+1)).concat("_");//add(i, metaInfo.get(String.valueOf(i+1)));
				payloadBucket.store(String.valueOf(i+1), chunks.get(i).toByteArray());
			}
			iovBucket.store(metaInfo.get("tag_name") + "_" + metaInfo.get("since"), chunkHashes);
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
		*/
	}

	@Override
	public void closeResources() throws CustomSamplersException {
		// TODO Auto-generated method stub
		
	}


}
