package riak;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import pojo.Payload;
import pojo.Tag;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.WalkResult;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
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
			throws CustomSamplersException, NotFoundInDBException {
		riakClient = RiakConfigElement.getRiakClient(clusterName);
		if (riakClient == null)
			throw new NotFoundInDBException("IRiakClient instance with name: " + clusterName 
					+ " was not found in config!");
	}

	@Deprecated
	@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			byte[] fileContent, boolean isSpecial) throws CustomSamplersException {
	}

	@Deprecated
	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial) 
			throws CustomSamplersException {
		return null;
	}

	@Override
	public void writePayload(HashMap<String, String> metaInfo, byte[] payload,
			byte[] streamerInfo, boolean isSpecial)
					throws CustomSamplersException {
		try {
			Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
			if (payload == null) {
				Payload pl = new Payload();
				pl.hash = metaInfo.get("payload_hash");
				pl.objectType = metaInfo.get("object_type");
				pl.data = new byte[0];
				pl.streamerInfo = streamerInfo;
				pl.version = metaInfo.get("version");
				pl.creationTime = System.currentTimeMillis();
				pl.cmsswRelease = metaInfo.get("cmssw_release");
			} else {
				IRiakObject pl = plBucket.store(metaInfo.get("payload_hash"), payload).execute();
				pl.addUsermeta("object_type", metaInfo.get("object_type"));
				pl.addUsermeta("streamer_info", streamerInfo.toString());
				pl.addUsermeta("version", metaInfo.get("version"));
				pl.addUsermeta("creation_time", String.valueOf(System.currentTimeMillis()));
				pl.addUsermeta("cmssw_release", metaInfo.get("cmssw_release"));
			}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public byte[] readPayload(String hashKey, boolean isSpecial)
			throws CustomSamplersException {
		try {
			Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject obj = plBucket.fetch(hashKey).execute();
			return obj.getValue();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public void writeChunk(HashMap<String, String> metaInfo, String chunkID,
			byte[] chunk, Boolean isSpecial) throws CustomSamplersException {
		try {
			Bucket b = riakClient.fetchBucket("CHUNK").execute();
			b.store(metaInfo.get(chunkID), chunk).execute();
			// Create LINK from Parent PL to CH.
			b = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject plObj = b.fetch(metaInfo.get("payload_hash")).execute();
			plObj.addLink(new RiakLink("CHUNK", metaInfo.get(chunkID), chunkID));
			b.store(plObj).execute();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public byte[] readChunk(String hashKey, String chunkHashKey,
			boolean isSpecial) throws CustomSamplersException {
		try {
			Bucket plBucket = riakClient.fetchBucket("CHUNK").execute();
			IRiakObject obj = plBucket.fetch(chunkHashKey).execute();
			return obj.getValue();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public byte[] readChunks(String hashKey, boolean isSpecial)
			throws CustomSamplersException {
		try {
			Bucket plBucket = riakClient.fetchBucket("PAYLOAD").execute();
			IRiakObject obj = plBucket.fetch(hashKey).execute();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			for (RiakLink link : obj.getLinks())
			{
				System.out.println(link.getBucket() + " " + link.getKey() + " " + link.getTag());
				IRiakObject chunk = riakClient.fetchBucket("CHUNK").execute()
											  .fetch(link.getKey()).execute();
				os.write(chunk.getValue());
			}
			return os.toByteArray();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured. Details: " + e.toString());
		}
	}

	@Override
	public void writeIov(HashMap<String, String> keyAndMetaMap)
			throws CustomSamplersException {
		try {
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(keyAndMetaMap.get("tag_name")).execute();
			tObj.addLink(new RiakLink("PAYLOAD", 
					keyAndMetaMap.get("payload_hash"), keyAndMetaMap.get("since")));
			b.store(tObj).execute();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}


	@Override
	public String readIov(HashMap<String, String> keyMap)
			throws CustomSamplersException {
		// WARNING: USE THIS WITH CARE!!!!!!!!!!!!!
		System.out.println("WARNING!!! USE THIS FUNCTION WITH CARE!");
		String result = null;
		try {
			Bucket b = riakClient.fetchBucket("TAG").execute();
			IRiakObject tObj = b.fetch(keyMap.get("tag_name")).execute();
			WalkResult wr = riakClient.walk(tObj).addStep("TAG", keyMap.get("since")).execute();
			Iterator<Collection<IRiakObject> > i = wr.iterator();
			int count = 0;
			while (i.hasNext())
			{
				count++;
				Collection<IRiakObject> c = i.next();
				for (IRiakObject o : c)
				{
					System.out.println(count + " " + o.getValueAsString());
					result = o.getKey();
				}
			}
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		} catch (RiakException e) {
			throw new CustomSamplersException("RiakException occured. Details: " + e.toString());
		}
		return result;
	}

	@Override
	public void writeTag(HashMap<String, String> metaMap)
			throws CustomSamplersException {
		Tag tagPojo = new Tag();
		tagPojo.name = metaMap.get("name");
		tagPojo.revision = Integer.parseInt(metaMap.get("revision"));
		tagPojo.revisionTime = Long.parseLong(metaMap.get("revision_time"));
		tagPojo.comment = metaMap.get("comment");
		tagPojo.timeType = Integer.parseInt(metaMap.get("time_type"));
		tagPojo.objectType = metaMap.get("object_type");
		tagPojo.lastValidated = Integer.parseInt(metaMap.get("last_validated"));
		tagPojo.endOfValidity = Integer.parseInt(metaMap.get("end_of_validity"));
		tagPojo.lastSince = Integer.parseInt(metaMap.get("last_since"));
		tagPojo.lastSincePid = Integer.parseInt(metaMap.get("last_since_pid"));
		tagPojo.creationTime = System.currentTimeMillis();
		try {
			Bucket tagBucket = riakClient.fetchBucket("TAG").execute();
			tagBucket.store(metaMap.get("name"), tagPojo).execute();
		} catch (RiakException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
	}

	@Override
	public HashMap<String, Object> readTag(String tagKey)
			throws CustomSamplersException {
		HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			Bucket tagBucket = riakClient.fetchBucket("TAG").execute();
			Tag tagPojo = tagBucket.fetch(tagKey, Tag.class).execute();
			result.put("revision", tagPojo.revision);
			result.put("revision_time", tagPojo.revisionTime);
			result.put("comment", tagPojo.comment);
			result.put("time_type", tagPojo.timeType);
			result.put("object_type", tagPojo.objectType);
			result.put("last_validated", tagPojo.lastValidated);
			result.put("end_of_validity", tagPojo.endOfValidity);
			result.put("last_since", tagPojo.lastSince);
			result.put("last_since_pid", tagPojo.lastSincePid);
			result.put("creation_time", tagPojo.creationTime);
		} catch (RiakException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
		return result;
	}

}
