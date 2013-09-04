package utils;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.jmeter.samplers.SampleResult;

public class CustomSamplerUtils {

  public static SampleResult getInitialSampleResult(String className) {
    SampleResult res = new SampleResult();
    res.setSampleLabel(className);
    res.setResponseCodeOK();
    res.setResponseCode("200");
    res.setSuccessful(true);
    res.setResponseMessageOK();
    res.setDataType(SampleResult.TEXT);
    res.setContentType("text/plain");
    return res;
  }

  public static int getThreadID(String threadName) {
    String[] elements = threadName.split("\\s+");
    String gAndTIDStr = elements[elements.length - 1];
    String[] groupAndThreadIDs = gAndTIDStr.split("-");
    return Integer.parseInt(groupAndThreadIDs[groupAndThreadIDs.length - 1]);
  }

  public static void finalizeResponse(SampleResult res, Boolean success,
          String code, String responseStr) {
    res.latencyEnd();
    res.setSuccessful(success);
    res.setResponseCode(code);
    res.setResponseMessage(responseStr);
    res.setResponseData(responseStr.getBytes());
  }

  public static void readWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
          SampleResult res, boolean isCheckRead, boolean isRandom, boolean isSpecial) {
    HashMap<String, String> meta = new HashMap<String, String>();
    if (isRandom) {
      meta = binaryInfo.getRandomMeta();
    } else {
      meta = binaryInfo.getAssignedMeta();
    }

    try {
      byte[] result = null;
      res.sampleStart();
      result = queryHandler.readPayload(meta, isSpecial);

      if (result == null)
        finalizeResponse(res, false, "500", "The result is empty!");

      //TODO: if (isCheckRead)
      //String filePath = binaryInfo.getBinaryFilePathList().get()
      finalizeResponse(res, true, "200", 
        "Payload read: " + meta.get("full_id") + " read successfully!");

    } catch (CustomSamplersException ex) {
      finalizeResponse(res, false, "500", ex.toString());
    } finally {
      res.sampleEnd();
    }
}

  public static void writeWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
          SampleResult res, boolean isAssigned, boolean isSpecial) {
    if (!isAssigned) { // Then binaryID is based on ThreadID.
      int threadID = getThreadID(Thread.currentThread().getName());
      String binaryID = binaryInfo.getXthBinaryID(threadID);  
      HashMap<String, String> meta = binaryInfo.getMetaInfo().get(binaryID + ".chunks");

      String binaryFullPath = binaryInfo.getOriginalFilePathList().get(binaryID);
      byte[] payload = binaryInfo.read(binaryFullPath);
      byte[] streamerInfo = binaryInfo.read(binaryFullPath + ".chunks/STREAMER_INFO.bin");
      try {
        res.sampleStart();
        queryHandler.writePayload(meta, payload, streamerInfo, isSpecial);
        finalizeResponse(res, true, "200",
                         "Payload write: " + binaryID + " Successfull!");
      } catch (CustomSamplersException ex) {
        finalizeResponse(res, false, "500", ex.toString());
      } finally {
        res.sampleEnd();
      }
    } else {
      // TODO
      // String binaryID = BinaryInfo.getAssignedIDForThread(threadID);
    }
  }

  public static void doReadWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
          SampleResult res, boolean isCheckRead, boolean isSpecial) {
    HashMap<String, String> hashes = binaryInfo.getRandomHashesAndIDs();
    String originalID = hashes.get("originalID");
    String chunkID = hashes.get("chunkID");

    try {
      byte[] result = null;	
      res.sampleStart();
      result = queryHandler.readBinary(originalID, chunkID,
              hashes.get("original") + "__" + hashes.get("chunk"), isSpecial);

      if (result == null)
        finalizeResponse(res, false, "500", "The result is empty!");

      if (isCheckRead) {
        String filePath = binaryInfo.getBinaryFilePathList().get(originalID).get(chunkID);
        byte[] fileContent = binaryInfo.read(filePath);
        if (!Arrays.equals(result, fileContent))
          finalizeResponse(res, false, "500", "Read value is not correct!");
        }

        finalizeResponse(res, true, "200",
                "Value read for:" + " B:" + originalID + " C:" + chunkID + " Success!");

    } catch (CustomSamplersException ex) {
      finalizeResponse(res, false, "500", ex.toString());
    } finally {
      res.sampleEnd();
    }
  }

	public static void doWriteWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo, 
			                       SampleResult res, boolean isAssigned, boolean isSpecial) {
		if (isAssigned) {
    		String chunkID = "chunk-" + getThreadID(Thread.currentThread().getName()) + ".bin";
    		String pathToChunk = binaryInfo.getBinaryFilePathList().get("BIGrbinary-0.bin.chunks").get(chunkID);
    		byte[] chunkContent = binaryInfo.read(pathToChunk);
    		HashMap<String, String> hashes = binaryInfo.getHashesForIDs("BIGrbinary-0.bin.chunks", chunkID);
    		try {
    			res.sampleStart();
    			queryHandler.writeBinary("BIGrbinary-0.bin.chunks", chunkID, 
    					hashes.get("original") + "__" + hashes.get("chunk"), chunkContent, isSpecial);
    			
    			finalizeResponse(res, true, "200",
    					"Value read for:" + " B: BIGrbinary-0.bin.chunks" + " C:" + chunkID + " Success!");
    			
			} catch (CustomSamplersException ex) {
				//log.error("PostgreSampler write attempt failed: " + ex.toString());
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		} else {
			// TODO
		}
	}
		
}
