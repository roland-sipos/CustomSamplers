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

import assignment.Assignment;
import binaryinfo.BinaryFileInfo;

/**
 * This utility class has several static methods and functions to unify
 * the different sampling scenarios based on the user options in the samplers.
 * It handles the initialization, finalization and also starting, pausing and
 * resuming the SampleResults. Also this class contains the main R/W processing
 * with the different QueryHandlers.
 * */
public class CustomSamplerUtils {

	/**
	 * One cannot create an instance of the class, as all of it's methods are static
	 * utility functions that are used by other packages.
	 * */
	private CustomSamplerUtils() {}

	/**
	 * The method returns with an initialized SampleResult. Intialization happens
	 * in a sense that the original error codes, content- and data-type, response 
	 * messages and labels are set to the CustomSampler's default values.
	 * 
	 * @param  className  the name of the caller class for label the Sample
	 * @return  SampleResult  an initialized SampleResult
	 * */
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

	/**
	 * This function can make a SampleResult, filled with the content of an Exception.
	 * The main use case of this, is when the prerequisites are fetched or created for the
	 * sampler. So in the case of bad setups (due to missing ConfigElements) will always
	 * result with a SampleResult that has 999 error code, and the result data with the
	 * Exception as a string.
	 * 
	 * @param  e  the Exception, why the exception result is requested
	 * @return  SampleResult  the SampleResult that contains the exception and error code 999
	 * */
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

	/**
	 * This method is finalizing the SampleResults that was get as a parameter.
	 * Firstly it stops the internal stopper, sets the success flag, error code and
	 * the response based on the parameters that were passed.
	 * 
	 * @param  res  the SampleResult to be finalized
	 * @param  success  the flag that indicates the successfulness of the Sample
	 * @param  code  the error-code to be set
	 * @param  responseStr  the response message to be set
	 * */
	public static void finalizeResponse(SampleResult res, Boolean success,
			String code, String responseStr) {
		res.latencyEnd();
		res.setSuccessful(success);
		res.setResponseCode(code);
		res.setResponseMessage(responseStr);
		//res.setResponseData(responseStr.getBytes());
	}

	/**
	 * This method parses a JMeter Thread's name to extract the ID of the thread
	 * in the actual thread group.
	 * <p>
	 * Note, that the thread group ID is ignored in this case, as there is no
	 * cross-thread group side-effect in this JMeter extension.
	 * 
	 * @param  threadName  the JMeter Thread's name to be parsed
	 * @return  int  the ID of the thread in the current thread-group
	 * */
	public static int getThreadID(String threadName) {
		String[] elements = threadName.split("\\s+");
		String gAndTIDStr = elements[elements.length - 1];
		String[] groupAndThreadIDs = gAndTIDStr.split("-");
		return Integer.parseInt(groupAndThreadIDs[groupAndThreadIDs.length - 1]);
	}

	/**
	 * The function merges the mapped ByteArrayOutputStream objects into one,
	 * based on the indicated number as the mapping represents chunk-number,
	 * chunk-data pairs.
	 * 
	 * @param  map  a mapping that contains chunk ID - chunk value pairs
	 * @return  ByteArrayOutputStream  the merged binary content of the mapping
	 * */
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

		} catch (IOException e) {
			throw new CustomSamplersException("IOException occured during array merge: " + e.toString());
		}
		return resOs;
	}

	/**
	 * The function is making a fast check of the equality between the binary content that
	 * is read from the database, and that is read from the original file. This function is called
	 * only if a Sampler requests the check (with the checkRead option).
	 * 
	 * @param  result  the content that is read from the database
	 * @param  assignment  the assignment object that was used for the operation
	 * @param  meta  the meta data of the binary file that was used for the operation
	 * @return  boolean  true if the content is the same, false if the content differs
	 * */
	private static boolean checkMatch(ByteArrayOutputStream result,
			Assignment assignment, HashMap<String, String> meta) {
		String binaryFullPath = assignment.getBinaryFileInfo().getFilePathList().get(meta.get("id"));
		ByteArrayOutputStream payload = assignment.getReader().read(binaryFullPath);
				//Readers.BinaryReader.read(binaryFullPath);
		return Arrays.equals(result.toByteArray(), payload.toByteArray());
	}

	/**
	 * This static utility method defines the read protocol between the project's Sampler
	 * classes and different database entities. It has different main phases as follows:
	 * 1. Fetch information from the Assignment object, based on the current thread ID.
	 * 2. Based on user options and the information from 1., it calls the appropriate read
	 * option of the QueryHandler that was passed.
	 * 3. Based on the result, it finalizes the sample.
	 * 
	 * @param  queryHandler  and implemented QueryHandler object that handles a given database
	 * @param  assignment  the Assignment object, to fetch the assigned informations
	 * @param  res  the SampleResult to be finalized based on the result
	 * @param  options  user options from the Sampler, that called this function
	 * */
	public static void readWith(QueryHandler queryHandler, Assignment assignment,
			SampleResult res, HashMap<String, Boolean> options) {
		// !!! CHECK THIS: Thread.currentThread().getId();
		HashMap<String, String> meta = assignment.getMeta(getThreadID(Thread.currentThread().getName()));
		res.setRequestHeaders(meta.get("id"));
		try {
			ByteArrayOutputStream result = null;
			Boolean chunkMode = options.get("useChunks");
			String tagName = meta.get("tag_name");
			Long since = Long.valueOf(meta.get("since"));
			if (chunkMode) {
				Map<Integer, ByteArrayOutputStream> data = new HashMap<Integer, ByteArrayOutputStream>();
				res.sampleStart();
				data = queryHandler.getChunks(tagName, since);
				res.samplePause();
				result = mergeToByteArrayOStream(data);
				/*for (Entry<String, HashMap<String, String>> entry : metaMap.entrySet())
				{
					HashMap<String, String> meta = entry.getValue();
					SampleResult subres = getInitialSampleResult(entry.getKey());
					subres.sampleStart();
					Map<Integer, ByteArrayOutputStream> data = queryHandler.getChunks(
							meta.get("tag_name"), Long.parseLong(meta.get("since")));
					subres.sampleEnd();
					res.addSubResult(subres);
				}*/
			} else {
				res.sampleStart();
				result = queryHandler.getData(tagName, since);
				res.samplePause();
			}

			if (result == null) {
				finalizeResponse(res, false, "500", "The result is empty for " + meta.get("id") + " !");
			} else {
				/** We will not save the huge BLOBs as response data, however we'll store their size. */
				//res.setResponseData(result.toByteArray());
				res.setBytes(result.size());
				if (options.get("validateOperation")) {
					if (!checkMatch(result, assignment, meta)) {
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
			ex.printStackTrace();
			finalizeResponse(res, false, "500", "Could not read Payload: " + meta.get("id")
					+ " Reason:" + ex.getMessage());
		} finally {
			res.sampleEnd();
		}
	}

	/**
	 * This static utility method defines the write protocol between the project's Sampler
	 * classes and different database entities. It has different main phases as follows:
	 * 1. Fetch information from the Assignment object, based on the current thread ID.
	 * 2. Based on user options and the information from 1., it calls the appropriate write
	 * option of the QueryHandler that was passed.
	 * 3. Based on QueryHandler responses, it finalizes the sample.
	 * 
	 * @param  queryHandler  and implemented QueryHandler object that handles a given database
	 * @param  assignment  the Assignment object, to fetch the assigned informations
	 * @param  res  the SampleResult to be finalized based on the result
	 * @param  options  user options from the Sampler, that called this function
	 * */
	public static void writeWith(QueryHandler queryHandler, Assignment assignment,
			SampleResult res, HashMap<String, Boolean> options) {
		HashMap<String, String> meta = assignment.getMeta(getThreadID(Thread.currentThread().getName()));
		String binaryID = meta.get("id");

		BinaryFileInfo binInfo = assignment.getBinaryFileInfo();

		String streamerInfoFullPath = binInfo.getPathForStreamerInfo(binaryID);
		ByteArrayOutputStream streamerInfo = assignment.getReader().read(streamerInfoFullPath);

		if (options.get("useChunks")) { // Write the chunks, not the big file.
			try {
				List<ByteArrayOutputStream> chunks =
						assignment.getReader().readChunks(binInfo.getChunkPathList().get(binaryID));
				res.sampleStart();
				queryHandler.putChunks(meta, chunks);
				res.samplePause();
				finalizeResponse(res, true, "200", "Payload (chunk) write: " + binaryID + " Successfull!");
			} catch (CustomSamplersException ex) {
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		} else { // Write the big file, not it's chunks.
			String binaryFullPath = binInfo.getAbsolutePathFor(binaryID);
			ByteArrayOutputStream payload = assignment.getReader().read(binaryFullPath);
			res.setBytes(payload.size());
			try {
				res.sampleStart();
				queryHandler.putData(meta, payload, streamerInfo);
				res.samplePause();
				finalizeResponse(res, true, "200", "Payload write: " + binaryID + " Successfull!");
			} catch (CustomSamplersException ex) {
				finalizeResponse(res, false, "500", ex.toString());
			} finally {
				res.sampleEnd();
			}
		}
	}

	/**
	 * The original read utility function that received the different options one-by-one.
	 * Unimplemented, will be removed soon.
	 * 
	 * @deprecated
	 * */
	@Deprecated
	public static void doReadWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo,
			SampleResult res, boolean isCheckRead, boolean isSpecial) {
	}

	/**
	 * The original write utility function that received the different options one-by-one.
	 * Unimplemented, will be removed soon.
	 * 
	 * @deprecated
	 * */
	@Deprecated
	public static void doWriteWith(QueryHandler queryHandler, BinaryFileInfo binaryInfo, 
			SampleResult res, boolean isAssigned, boolean isSpecial) {
	}

}
