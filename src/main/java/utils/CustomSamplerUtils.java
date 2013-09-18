package utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
			SampleResult res, HashMap<String, Boolean> options) {
			//SampleResult res, boolean isCheckRead, boolean isRandom, boolean isSpecial) {
		HashMap<String, String> meta = new HashMap<String, String>();
		if (options.get("isRandom")) {
			meta = binaryInfo.getRandomMeta();
		} else {
			meta = binaryInfo.getAssignedMeta(1);
		}
		try {
			res.sampleStart();
			String hash = queryHandler.readIov(meta);
			byte[] result = null;
			result = queryHandler.readPayload(hash, options.get("isSpecial"));

			if (result == null) {
				finalizeResponse(res, false, "500", "The result is empty for " + meta.get("id") + " !");
			} else {
				if (options.get("isCheckRead")) {
					String binaryFullPath = binaryInfo.getFilePathList().get(meta.get("id"));
					byte[] payload = binaryInfo.read(binaryFullPath);
					if (result.equals(payload)) {
						finalizeResponse(res, false, "600",
								"Payload content for: " + meta.get("id") + " differs from the original!");
					} else {
						finalizeResponse(res, true, "200",
								"Payload read: " + meta.get("id") + " read successfully and matching with original!");
					}
				} else {
					finalizeResponse(res, true, "200",
							"Payload read: " + meta.get("id") + " read successfully!");
				}
			}
		} catch (CustomSamplersException ex) {
			finalizeResponse(res, false, "500", ex.toString());
		} finally {
			res.sampleEnd();
		}
	}

	public static void writeWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
			SampleResult res, HashMap<String, Boolean> options) {
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
		byte[] streamerInfo = binaryInfo.read(streamerInfoFullPath);

		if (options.get("useChunks")) { // Write the chunks, not the big file.
			TreeMap<String, String> chunkPathList = binaryInfo.getChunkPathList().get(binaryID);
			try {
				res.sampleStart();
				queryHandler.writePayload(binaryMeta, null, streamerInfo, options.get("isSpecial"));
				res.samplePause();
				for (Map.Entry<String, String> it : chunkPathList.entrySet()) {
					byte[] chunk = binaryInfo.read(it.getValue());
					SampleResult subres = getInitialSampleResult(binaryID + " - " + it.getKey());
					subres.sampleStart();
					queryHandler.writeChunk(binaryMeta, it.getKey(), chunk, options.get("isSpecial"));
					finalizeResponse(subres, true, "200", "Chunk write: " + it.getKey() + " Successfull!");
					subres.sampleEnd();
					res.storeSubResult(subres);
				}
				res.sampleResume();
				queryHandler.writeIov(binaryMeta);
				finalizeResponse(res, true, "200", "Payload (chunk) write: " + binaryID + " Successfull!");
			} catch (CustomSamplersException ex) {
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		} else { // Write the big file, not it's chunks.
			String binaryFullPath = binaryInfo.getFilePathList().get(binaryID);
			byte[] payload = binaryInfo.read(binaryFullPath);
			try {
				res.sampleStart();
				queryHandler.writePayload(binaryMeta, payload, streamerInfo, options.get("isSpecial"));
				queryHandler.writeIov(binaryMeta);
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
