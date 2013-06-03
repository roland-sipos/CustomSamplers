package riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.operations.StoreObject;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class RiakQueryHandler implements QueryHandler {

	private static IRiakClient riakClient;
	private static Bucket bucket;

	protected static class BinaryPojo {
		public BinaryPojo() { }
		public BinaryPojo(String original, String chunk, byte[] b) {
			this.originalID = original;
			this.chunkID = chunk;
			this.blob = b;
		}
		//@RiakKey public String hash;
		private String originalID;
		private String chunkID;
		private byte[] blob;
		
		public byte[] getBlob() { return this.blob; }
	}
	
	/*protected class KryoBinaryPojoConverter implements Converter<BinaryPojo> {
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
	
	public RiakQueryHandler(String clusterName, String bucketName) 
			throws CustomSamplersException, NotFoundInDBException {
		riakClient = RiakConfigElement.getRiakClient(clusterName);
		if (riakClient == null)
			throw new NotFoundInDBException("IRiakClient instance with name: " + clusterName 
					                        + " was not found in config!");		
		try {
			bucket = riakClient.fetchBucket(bucketName).execute();
		} catch (RiakRetryFailedException e) {
			throw new NotFoundInDBException("Exception occured during bucket fetch attempt!" 
		                                    + " Bucket name: " + bucketName 
                                            + " IRiakClient:" + riakClient.toString()
                                            + " Exception: " + e.toString());
		}
	}
	
	@Override
	public void writeBinary(String binaryID, String chunkID, String hash,
			                byte[] fileContent, boolean isSpecial) throws CustomSamplersException {
		System.out.println("WOOF -> I'll WRITE WITH KEY: " + hash);
		StoreObject<IRiakObject> sObj = bucket.store(hash, fileContent);
		try {
			IRiakObject res = sObj.pr(1).r(1).pw(1).w(1).dw(1).returnBody(false).execute();
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
	}

	@Override
	public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial) 
			throws CustomSamplersException {
		//BinaryPojo binaryPojo = null;
		byte[] ret = null;
		try {
			IRiakObject obj = bucket.fetch(hash).r(Quora.ONE).execute();
			ret = obj.getValue();
		} catch (UnresolvedConflictException e) {
			throw new CustomSamplersException("UnresolvedConflictException occured. Details: " + e.toString());
		} catch (RiakRetryFailedException e) {
			throw new CustomSamplersException("RiakRetryFailedException occured. Details: " + e.toString());
		} catch (ConversionException e) {
			throw new CustomSamplersException("ConversionException occured. Details: " + e.toString());
		}
		return ret;
	}

}
