package utils;

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

	public static void finalizeResponse(SampleResult res, Boolean success, String code, String responseStr) {
		res.latencyEnd();
		res.setSuccessful(success);
		res.setResponseCode(code);
		res.setResponseMessage(responseStr);
		res.setResponseData(responseStr.getBytes());
	}
	
}
