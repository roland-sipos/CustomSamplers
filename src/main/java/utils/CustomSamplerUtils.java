package utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

	public static SampleResult getExceptionSampleResult(Exception e) {
		SampleResult res = new SampleResult();
		res.setSampleLabel("ExceptionOccured");
		res.setResponseCode("999");
		res.setSuccessful(false);
		res.setRequestHeaders(e.getMessage());
		res.setResponseMessage(e.toString());
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

	private static ByteArrayOutputStream mergeToByteArrayOStream(
			Map<Integer, ByteArrayOutputStream> map)
					throws CustomSamplersException {
		ByteArrayOutputStream resOs = new ByteArrayOutputStream();
		try {
			List<ByteArrayOutputStream> listRes = new ArrayList<ByteArrayOutputStream>();
			for (int i = 0; i < map.entrySet().size(); ++i) {
				listRes.add(i, null);
			}
			
			Iterator<Map.Entry<Integer, ByteArrayOutputStream> > mIt = map.entrySet().iterator(); 
			while (mIt.hasNext()) {
				Map.Entry<Integer, ByteArrayOutputStream> entry = mIt.next();
				listRes.set(entry.getKey()-1, entry.getValue());
			}
			
			Iterator<ByteArrayOutputStream> lIt = listRes.iterator(); 
			while (lIt.hasNext()) {
				ByteArrayOutputStream ba = lIt.next();
				resOs.write(ba.toByteArray());
			}

		} catch (IOException e){
			throw new CustomSamplersException("IOException occured during array merge: " + e.toString());
		}	
		return resOs;
	}
	
	public static void readWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
			SampleResult res, HashMap<String, Boolean> options) {
		/*if (options.get("isSpecial")) {
			//PASS EVERYTHING TO THE QUERYHANDLER!!!
			queryHandler.readWith(binaryInfo, res, options);
		}*/

		HashMap<String, String> meta = new HashMap<String, String>();
		if (options.get("isRandom")) {
			meta = binaryInfo.getRandomMeta();
		} else {
			meta = binaryInfo.getAssignedMeta();
		}
		res.setRequestHeaders(meta.toString());
		try {
			ByteArrayOutputStream result = null;
			Boolean chunkMode = options.get("useChunks");
			if (chunkMode) {
				Map<Integer, ByteArrayOutputStream> data = new HashMap<Integer, ByteArrayOutputStream>();
				res.sampleStart();
				data = queryHandler.getChunks(meta.get("tag_name"), Long.parseLong(meta.get("since")));
				res.samplePause();
				result = mergeToByteArrayOStream(data);
			} else {
				res.sampleStart();
				result = queryHandler.getData(meta.get("tag_name"), Long.parseLong(meta.get("since")));
				res.samplePause();
			}

			if (result == null) {
				finalizeResponse(res, false, "500", "The result is empty for " + meta.get("id") + " !");
			} else {
				if (options.get("isCheckRead")) {
					if (!checkMatch(result, binaryInfo, meta)) {
						finalizeResponse(res, false, "600", "Payload content for: " + meta.get("id")
								+ " differs from the original! (Chunks?:" + chunkMode.toString() + ")");
					} else {
						finalizeResponse(res, true, "200", "Payload read: " + meta.get("id")
								+ " read successfully and matching with original! "
								+ "(Chunks?:" + chunkMode.toString() + ")");
					}
				} else {
					finalizeResponse(res, true, "200", "Payload read: " + meta.get("id")
							+ " read successfully! (Chunks?:" + chunkMode.toString() + ")");
				}
			}
		} catch (CustomSamplersException ex) {
			finalizeResponse(res, false, "500", ex.toString());
		} finally {
			res.sampleEnd();
		}
	}

	private static boolean checkMatch(ByteArrayOutputStream result, BinaryFileInfo binaryInfo, 
			HashMap<String, String> meta) {
		String binaryFullPath = binaryInfo.getFilePathList().get(meta.get("id"));
		ByteArrayOutputStream payload = binaryInfo.read(binaryFullPath);
		return Arrays.equals(result.toByteArray(), payload.toByteArray());
	}

	public static void writeWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
			SampleResult res, HashMap<String, Boolean> options) {
		/*if (options.get("isSpecial")) {
			queryHandler.writeWith();
		}*/
		
		String binaryID = null;
		HashMap<String, String> binaryMeta = null;
		if (!options.get("isAssigned")) { // Then binaryID is based on ThreadID.
			int threadID = getThreadID(Thread.currentThread().getName());
			binaryID = binaryInfo.getXthFileName(threadID);
			binaryMeta = binaryInfo.getMetaInfo().get(binaryID);
		} else {
			// TODO: String binaryID = BinaryInfo.getAssignedIDForThread(threadID);
		}
		String streamerInfoFullPath = binaryInfo.getPathForStreamerInfo(binaryID);
		ByteArrayOutputStream streamerInfo = binaryInfo.read(streamerInfoFullPath);

		if (options.get("useChunks")) { // Write the chunks, not the big file.
			//TreeMap<String, String> chunkPathList = binaryInfo.getChunkPathList().get(binaryID);
			try {
				res.sampleStart();
				queryHandler.putChunks(binaryMeta, binaryInfo.readChunksFor(binaryID));
				//queryHandler.putData(binaryMeta, null, streamerInfo);
				//res.samplePause();
				/*for (Map.Entry<String, String> it : chunkPathList.entrySet()) {
					ByteArrayOutputStream chunk = binaryInfo.read(it.getValue());
					SampleResult subres = getInitialSampleResult(binaryID + " - " + it.getKey());
					Integer cId = binaryInfo.getIDForChunk(binaryID, it.getKey());
					
					subres.sampleStart();
					queryHandler.putChunk(binaryMeta, cId, chunk);
					finalizeResponse(subres, true, "200", "Chunk write: " + it.getKey() + " Successfull!");
					subres.sampleEnd();
					res.storeSubResult(subres);
				}*/
				//res.sampleResume();
				finalizeResponse(res, true, "200", "Payload (chunk) write: " + binaryID + " Successfull!");
			} catch (CustomSamplersException ex) {
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		} else { // Write the big file, not it's chunks.
			String binaryFullPath = binaryInfo.getFilePathList().get(binaryID);
			ByteArrayOutputStream payload = binaryInfo.read(binaryFullPath);
			try {
				res.sampleStart();
				queryHandler.putData(binaryMeta, payload, streamerInfo);
				finalizeResponse(res, true, "200", "Payload write: " + binaryID + " Successfull!");
			} catch (CustomSamplersException ex) {
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		}
	}

	@Deprecated
	public static void doReadWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
			SampleResult res, boolean isCheckRead, boolean isSpecial) {
	}

	@Deprecated
	public static void doWriteWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo, 
			SampleResult res, boolean isAssigned, boolean isSpecial) {
	}

}
