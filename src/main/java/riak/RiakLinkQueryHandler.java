package riak;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.WalkResult;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class RiakLinkQueryHandler implements QueryHandler {

	private static IRiakClient riakClient;

	public RiakLinkQueryHandler(String clusterName) 
			throws CustomSamplersException, NotFoundInDBException {
		if (RiakConfigElement.getProtocolName().equals("HTTP")) {
			riakClient = null;
			throw new CustomSamplersException("IRiakClient instance with name: " + clusterName
					+ " is using HTTP protocol! The LinkWalk method did not work during the testing"
					+ " phase of the riak package in the CustomSamplers project!");
		} else {
			riakClient = RiakConfigElement.getRiakClient(clusterName);
		}
		if (riakClient == null)
			throw new NotFoundInDBException("IRiakClient instance with name: " + clusterName 
					+ " was not found in config!");
	}

	@Override
	public ByteArrayOutputStream getData(String tagName, long since)
			throws CustomSamplersException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(tagName).execute();
			WalkResult wr = riakClient.walk(tObj)
					.addStep("PAYLOAD", Long.toString(since), true).execute();
			Iterator<Collection<IRiakObject> > i = wr.iterator();
			int count = 0;
			while (i.hasNext())
			{
				count++;
				Collection<IRiakObject> c = i.next();
				for (IRiakObject o : c)
				{
					//System.out.println(count + ". link shows to " + o.getKey());
					// Skipping the PAYLOAD bucket, to fetch META for this payload...
					IRiakObject plObj = riakClient.fetchBucket("CHUNK").execute().fetch(o.getKey()).execute();
					result.write(plObj.getValue());
				}
			}
			if (count > 1) {
				throw new CustomSamplersException("More than one link found for " 
						+ "TAG=" + tagName + " SINCE=" + String.valueOf(since));
			}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (RiakException e) {
			throw new CustomSamplersException("RiakException occured. Details: " + e.toString());
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
			Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
			// The PAYLOAD bucket is just meta-bucket for the actual hash.
			StoreObject<IRiakObject> sObj = plBucket.store(metaInfo.get("payload_hash"), metaInfo.get("payload_hash"));
			IRiakObject pl = sObj.returnBody(true).execute();
			pl.addUsermeta("object_type", metaInfo.get("object_type"));
			pl.addUsermeta("streamer_info", String.valueOf(streamerInfo));
			pl.addUsermeta("version", metaInfo.get("version"));
			pl.addUsermeta("creation_time", String.valueOf(System.currentTimeMillis()));
			pl.addUsermeta("cmssw_release", metaInfo.get("cmssw_release"));
			plBucket.store(pl).execute();

			// The actual payload binary is inside the CHUNK bucket.
			pl.addLink(new RiakLink("CHUNK", metaInfo.get("payload_hash"), "FULL"));
			Bucket c = riakClient.fetchBucket("CHUNK").execute();
			c.store(metaInfo.get("payload_hash"), payload.toByteArray()).execute();

			// Add the IOV as a Link from TAG to the actual PAYLOAD.
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(metaInfo.get("tag_name")).execute();
			//StoreObject<IRiakObject> tObj = b.fetch(keyAndMetaMap.get("tag_name")).execute();
			tObj.addLink(new RiakLink("PAYLOAD", 
					metaInfo.get("payload_hash"), metaInfo.get("since"))); // KEY in the BUCKET, LINK tag.
			b.store(tObj).returnBody(false).execute();

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
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(tagName).execute();
			WalkResult wr = riakClient.walk(tObj)
					.addStep("PAYLOAD", Long.toString(since)).execute();
			Iterator<Collection<IRiakObject> > i = wr.iterator();
			int count = 0;
			while (i.hasNext())
			{
				count++;
				Collection<IRiakObject> c = i.next();
				for (IRiakObject o : c)
				{
					// Get the LINK list of the object.
					List<RiakLink> links = o.getLinks();
					Bucket cBuck = riakClient.fetchBucket("CHUNK").execute();
					// Iterate through the links, and ...
					for (int j = 0; j < links.size(); ++j) {
						// ... fetch the pointed CHUNK ...
						IRiakObject cObj = cBuck.fetch(links.get(j).getKey()).execute();
						// ... write it to a ByteArrayOutputStream ...
						ByteArrayOutputStream cS = new ByteArrayOutputStream();
						cS.write(cObj.getValue());
						// ... add the stream, to the result, with the ID as a key.
						result.put(Integer.parseInt(links.get(j).getTag()), cS);
					}
				}
			}
			if (count > 1) {
				throw new CustomSamplersException("More than one link found for " 
						+ "TAG=" + tagName + " SINCE=" + String.valueOf(since));
			}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (RiakException e) {
			throw new CustomSamplersException("RiakException occured. Details: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured. Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo,
			List<ByteArrayOutputStream> chunks) throws CustomSamplersException {
		try {
			// Store PAYLOAD as pure meta.
			Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
			StoreObject<IRiakObject> sObj = plBucket.store(metaInfo.get("payload_hash") , metaInfo.get("payload_hash"));
			IRiakObject pl = sObj.returnBody(true).execute();
			pl.addUsermeta("object_type", metaInfo.get("object_type"));
			pl.addUsermeta("streamer_info", "streamer_info");
			pl.addUsermeta("version", metaInfo.get("version"));
			pl.addUsermeta("creation_time", String.valueOf(System.currentTimeMillis()));
			pl.addUsermeta("cmssw_release", metaInfo.get("cmssw_release"));

			// Store CHUNKS, and add PAYLOAD -> CHUNK Links.
			Bucket cB = riakClient.fetchBucket("CHUNK").execute();
			for (int i = 0; i < chunks.size(); ++i) {
				String hash = metaInfo.get(String.valueOf(i+1));
				pl.addLink(new RiakLink("CHUNK", hash, String.valueOf(i+1)));
				plBucket.store(pl).returnBody(false).execute();
				cB.store(hash, chunks.get(i).toByteArray()).execute();
			}

			// Add the IOV as a Link from TAG to the actual PAYLOAD.
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(metaInfo.get("tag_name")).execute();
			//StoreObject<IRiakObject> tObj = b.fetch(keyAndMetaMap.get("tag_name")).execute();
			tObj.addLink(new RiakLink("PAYLOAD", 
					metaInfo.get("payload_hash"), metaInfo.get("since"))); // KEY in the BUCKET, LINK tag.
			b.store(tObj).returnBody(false).execute();

		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

}
