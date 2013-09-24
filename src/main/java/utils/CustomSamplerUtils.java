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
		HashMap<String, String> meta = new HashMap<String, String>();
		if (options.get("isRandom")) {
			meta = binaryInfo.getRandomMeta();
		} else {
			meta = binaryInfo.getAssignedMeta(1);
		}
		try {
			byte[] result = null;
			Boolean chunkMode = options.get("useChunks");
			if (chunkMode) {
				res.sampleStart();
				result = queryHandler.getChunks(meta.get("tag_name"), Long.parseLong(meta.get("since")));
			} else {
				res.sampleStart();
				result = queryHandler.getData(meta.get("tag_name"), Long.parseLong(meta.get("since")));
			}
			res.samplePause();

			if (result == null) {
				finalizeResponse(res, false, "500", "The result is empty for " + meta.get("id") + " !");
			} else {
				if (options.get("isCheckRead")) {
					if (checkMatch(result, binaryInfo, meta)) {
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

	private static boolean checkMatch(byte[] result, BinaryFileInfo binaryInfo, 
			HashMap<String, String> meta) {
		String binaryFullPath = binaryInfo.getFilePathList().get(meta.get("id"));
		byte[] payload = binaryInfo.read(binaryFullPath);
		return result.equals(payload);
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
				queryHandler.putData(binaryMeta, null, streamerInfo);
				res.samplePause();
				for (Map.Entry<String, String> it : chunkPathList.entrySet()) {
					byte[] chunk = binaryInfo.read(it.getValue());
					SampleResult subres = getInitialSampleResult(binaryID + " - " + it.getKey());
					subres.sampleStart();
					queryHandler.putChunk(binaryMeta, it.getKey(), chunk);
					finalizeResponse(subres, true, "200", "Chunk write: " + it.getKey() + " Successfull!");
					subres.sampleEnd();
					res.storeSubResult(subres);
				}
				res.sampleResume();
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
