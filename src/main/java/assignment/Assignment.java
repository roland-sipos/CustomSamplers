package assignment;

import java.util.HashMap;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import utils.CustomSamplersException;

import binaryconfig.BinaryFileInfo;

public class Assignment {

	public enum Mode {
		ASSIGNED, /* Provide files only for threads that are defined in the assignment map. */
		RANDOM, /* Ignore the map, and provide random files for the threads. */
		MIXED /* The mix of the first two. If threadID is not defined, it get's a random file. */
	}

	private static String assignFilePath;
	private static Mode assignmentMode;
	private static int numOfThreads;
	private static BinaryFileInfo binaryFileInfo;
	private static HashMap<Integer, SortedSet<String>> assignmentMap;

	public HashMap<Integer, SortedSet<String>> getAssignments() {
		return assignmentMap;
	}
	public Mode getAssignmentMode() {
		return assignmentMode;
	}
	public BinaryFileInfo getBinaryFileInfo() {
		return binaryFileInfo;
	}

	private void setModeFromString(String modeStr) throws CustomSamplersException {
		if (modeStr == "assigned") {
			assignmentMode = Mode.ASSIGNED;
		} else if (modeStr == "random") {
			assignmentMode = Mode.RANDOM;
		} else if (modeStr == "mixed") {
			assignmentMode = Mode.MIXED;
		} else {
			throw new CustomSamplersException("Unknown Mode: " + modeStr
					+ " Cannot create Assignment!");
		}
	}

	private void finalizeAssignments() throws CustomSamplersException {
		if (assignmentMode == Mode.ASSIGNED) {
			assignmentMap = AssignmentXMLParser.parse(assignFilePath);
		} else if (assignmentMode == Mode.RANDOM) {
			Random random = new Random();
			String[] fileNames = binaryFileInfo.getFileNameArray();
			for (int i = 0; i < numOfThreads; ++i) {
				SortedSet<String> set = new TreeSet<String>();
				set.add(fileNames[random.nextInt(fileNames.length)]);
				assignmentMap.put(i+1, set);
			}
		} else if (assignmentMode == Mode.MIXED) {
			assignmentMap = AssignmentXMLParser.parse(assignFilePath);
			Random random = new Random();
			String[] fileNames = binaryFileInfo.getFileNameArray();
			for (int i = 0; i < numOfThreads; ++i) {
				if (!assignmentMap.containsKey(i+1)) {
					SortedSet<String> set = new TreeSet<String>();
					set.add(fileNames[random.nextInt(fileNames.length)]);
					assignmentMap.put(i+1, set);
				}
			}
		}
	}

	public Assignment(String filePath, String mode, int threadNum, BinaryFileInfo binInfo)
			throws CustomSamplersException {
		assignFilePath = filePath;
		setModeFromString(mode);
		numOfThreads = threadNum;
		binaryFileInfo = binInfo;
		finalizeAssignments();
	}

	/*public HashMap<String, HashMap<String, String>> getMetaFor(int threadID) {
		HashMap<String, HashMap<String, String>> res = new HashMap<String, HashMap<String, String>>();
		SortedSet<String> binaries = assignmentMap.get(threadID);
		for (String binaryID : binaries) {
			res.put(binaryID, binaryFileInfo.getMetaInfo().get(binaryID));
		}
		return res;
	}*/
	public HashMap<String, String> getMeta(int threadID) {
		SortedSet<String> binaries = assignmentMap.get(threadID);
		return binaryFileInfo.getMetaInfo().get(binaries.first());
	}

}
