package assignment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import utils.CustomSamplersException;

import binaryinfo.BinaryFileInfo;
import binaryinfo.Readers;
import binaryinfo.Readers.CustomReader;

/**
 * This class is holding assignment information, associated with a BinaryFileInfo instance.
 * Basically from a given configuration XML file, it creates a mapping between thread ID, and
 * binary file name pairs. In a single test plan, there may be many Assignment objects available
 * for a single instance of BinaryFileInfo.
 * */
public class Assignment {

	/**
	 * Enum for different assignment modes.
	 * */
	public enum Mode {
		/** Provide files only for threads that are defined in the assignment map. */
		ASSIGNED,
		/** Ignore the map, and provide random files for the threads. */
		RANDOM,
		/** The mix of ASSIGNED and RANDOM. If threadID is not defined in the assignment XML file,
		 * it get's a random file instead. */
		MIXED,
		/** Ignore the map, and creates a simple threadID = binary file index from the file array. */
		SEQUENCE
	}

	/** Full path of the assignment XML input configuration of this instance. */
	private String inputAssignFilePath;
	/** Full path of the assignment XML output configuration of this instance. */
	private String outputAssignFilePath;
	/** User option driven mode, set when the instance is created. */
	private Mode assignmentMode;
	/** The number of thread, when the instance was created from the ConfigElement. */
	private int numOfThreads;
	/** The reader instance, based on the encoding mode. */
	private CustomReader reader;
	/** The ID of the BinaryFileInfo resource. Normally a BinaryConfigElement's binaryInfo property. */
	private BinaryFileInfo binaryFileInfo;
	/** The final assignment map, that contains the thread ID - file name set pairs. */
	private HashMap<Integer, SortedSet<String>> assignmentMap;
	/** The final assignment map, that contains the thread ID - file path set pairs. */
	private HashMap<Integer, SortedSet<String>> assignedPathMap;

	/** Getter function for the assignmentMap.
	 * @return  HashMap<Integer, SortedSet<String>>  the assignmentMap field */
	public HashMap<Integer, SortedSet<String>> getAssignments() {
		return assignmentMap;
	}

	/** Getter function for the assignmentMode.
	 * @return  Mode  the assignmentMode field */
	public Mode getAssignmentMode() {
		return assignmentMode;
	}

	/** Getter function for the associated BinaryFileInfo.
	 * @return  BinaryFileInfo  the binaryFileInfo field */
	public BinaryFileInfo getBinaryFileInfo() {
		return binaryFileInfo;
	}

	/**
	 * Getter function for the encoding associated CustomReader instance. 
	 * @return  CustomReader  the encoding related CustomReader
	 * */
	public CustomReader getReader() {
		return reader;
	}

	/**
	 * An assignment object is constructed, based on several factors. Namely the full path to the
	 * assignment configuration (may be empty), the assignment mode as a string, the number of threads
	 * in the test plan where this resource is created, and finally a BinaryFileInfo object to create
	 * the association between the two instance. During construction, the assignment mapping is
	 * finalized, based on these parameters.
	 * 
	 * @param  filePath  the full path to the assignment XML configuration file
	 * @param  mode  the assignment mode as a string
	 * @param  threadNum  the number of threads to create assignments for
	 * @param  binInfo  the associated BinaryFileInfo object for the assignments
	 * @throws  CustomSamplersException  if the assignment creation failed, or was not possible at all
	 * */
	public Assignment(String iFilePath, String oFilePath, String mode, int threadNum,
			String encoding, BinaryFileInfo binInfo)
					throws CustomSamplersException {
		inputAssignFilePath = iFilePath;
		outputAssignFilePath = oFilePath;
		setModeFromString(mode);
		numOfThreads = threadNum;
		setReaderFromEncoding(encoding);
		binaryFileInfo = binInfo;
		assignmentMap = new HashMap<Integer, SortedSet<String>>();
		assignedPathMap = new HashMap<Integer, SortedSet<String>>();
		sanityCheck();
		finalizeAssignments();
	}

	/**
	 * This utility method sets the appropriate Mode Enum, based on the user option string.
	 * @param  modeStr  the mode, as a string
	 * @throws  CustomSamplersException  if the modeStr was not recognized as a valid assignment mode
	 * */
	private void setModeFromString(String modeStr) throws CustomSamplersException {
		if (modeStr.equals("assigned")) {
			assignmentMode = Mode.ASSIGNED;
		} else if (modeStr.equals("random")) {
			assignmentMode = Mode.RANDOM;
		} else if (modeStr.equals("mixed")) {
			assignmentMode = Mode.MIXED;
		} else if (modeStr.equals("sequence")) {
			assignmentMode = Mode.SEQUENCE;
		} else {
			throw new CustomSamplersException("Unknown Mode: " + modeStr
					+ " Cannot create Assignment!");
		}
	}

	/**
	 * This utility method sets the appropriate Encoding Enum, based on the user option string.
	 * @param  encodingStr  the encoding option, as a string
	 * @throws  CustomSamplersException  if the encodingStr was not recognized as a valid encoding mode
	 * */
	private void setReaderFromEncoding(String encodingStr) throws CustomSamplersException {
		if (encodingStr.equals("binary")) {
			reader = new Readers.BinaryReader();
		} else if (encodingStr.equals("base64")) {
			reader = new Readers.Base64Reader();
		} else {
			throw new CustomSamplersException("Unknown Encoding: " + encodingStr
					+ " Cannot create Assignment!");
		}
	}

	/**
	 * This method basically creates the final version the assignmentMap, as based on
	 * user options, there are several ways to create the mapping.
	 * <p>
	 * 1. Assigned: In this case, the map only contains the the information that the
	 * assignment XML file hold. <br>
	 * 2. Random: This version completely ignores the assignment XML file, and for
	 * every thread ID, pairs a random binary file name from the file name array. <br>
	 * 3. Mixed: This case mixes 1. and 2. on a way, that it adds a random file name for
	 * the thread IDs that were not defined in the assignment XML file. <br>
	 * 4. Sequence: This last version completely ignores the assignment XML file, and for
	 * every thread ID, pairs the equivalent indexed file name in the array.
	 * 
	 * @throws  CustomSamplersAssignment  if the assignment XML file parsing failed
	 * */
	private void finalizeAssignments() throws CustomSamplersException {
		if (assignmentMode == Mode.ASSIGNED) {
			assignmentMap = AssignmentXMLHandler.parse(inputAssignFilePath);
		} else if (assignmentMode == Mode.RANDOM) {
			Random random = new Random();
			String[] fileNames = binaryFileInfo.getFileNameArray();
			for (int i = 0; i < numOfThreads; ++i) {
				SortedSet<String> set = new TreeSet<String>();
				set.add(fileNames[random.nextInt(fileNames.length)]);
				assignmentMap.put(i+1, set);
			}
		} else if (assignmentMode == Mode.MIXED) {
			assignmentMap = AssignmentXMLHandler.parse(inputAssignFilePath);
			Random random = new Random();
			String[] fileNames = binaryFileInfo.getFileNameArray();
			for (int i = 0; i < numOfThreads; ++i) {
				if (!assignmentMap.containsKey(i+1)) {
					SortedSet<String> set = new TreeSet<String>();
					set.add(fileNames[random.nextInt(fileNames.length)]);
					assignmentMap.put(i+1, set);
				}
			}
		} else if (assignmentMode == Mode.SEQUENCE) {
			String[] fileNames = binaryFileInfo.getFileNameArray();
			for (int i = 0; i < numOfThreads; ++i) {
				SortedSet<String> set = new TreeSet<String>();
				set.add(fileNames[i]);
				assignmentMap.put(i+1, set);
			}
		}

		/** Based on the found binary file names, we look up the path of those also. */
		Iterator<Entry<Integer, SortedSet<String>>> it = assignmentMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, SortedSet<String>> assignment = it.next();
			Iterator<String> setIt = assignment.getValue().iterator();

			SortedSet<String> pathSet = new TreeSet<String>();
			while (setIt.hasNext()) {
				String fileName = setIt.next();
				String pathForFile = binaryFileInfo.getAbsolutePathFor(fileName);
				if (pathForFile == null) {
					throw new CustomSamplersException("Faild to finalize the assignment, because a"
							+ " file is unknown for the BinaryFileInfo!"
							+ " File name: " + fileName);
				} else {
					pathSet.add(pathForFile);
				}
			}
			assignedPathMap.put(new Integer(assignment.getKey()), pathSet);
		}

		/** If outputFile is given, we'll create an assignmentFile. */
		if (!outputAssignFilePath.equals("")) {
			AssignmentXMLHandler.build(outputAssignFilePath, assignmentMap);
		}

	}

	/**
	 * The method does a sanity check for the worst case scenarios, and throws and exception
	 * in order to prevent the resource creation itself. These scenarios are the following:
	 * <p>
	 * 1. Assignment mode is Sequence, but the number of files are less than the number of threads.
	 * 2. Assignment mode is Assigned or Mixed, however the assignment XML file contains invalid
	 * file names, about the BinaryFileInfo does not hold any information.
	 * 
	 * @throws  CustomSamplersException  if the sanity check did not pass
	 * */
	private void sanityCheck() throws CustomSamplersException {
		int numOfFiles = binaryFileInfo.getFileNameArray().length;
		System.out.println("NUM OF FILE:" + numOfFiles);
		if (assignmentMode == Mode.SEQUENCE && numOfFiles < numOfThreads) {
			throw new CustomSamplersException("Sanity check failed! The number of files are less "
					+ "than the number of threads, and the assignmentMode was set to sequence!");
		} else if ((assignmentMode == Mode.ASSIGNED || assignmentMode == Mode.MIXED)
				&& inputAssignFilePath.equals("")) {
			throw new CustomSamplersException("Sanity check failed! Assignment mode requires "
					+ "the path to the assignment file, but no such file given!");
		}

	}

	/*public HashMap<String, HashMap<String, String>> getMetaFor(int threadID) {
		HashMap<String, HashMap<String, String>> res = new HashMap<String, HashMap<String, String>>();
		SortedSet<String> binaries = assignmentMap.get(threadID);
		for (String binaryID : binaries) {
			res.put(binaryID, binaryFileInfo.getMetaInfo().get(binaryID));
		}
		return res;
	}*/
	/**
	 * This function bypasses the meta information fetching between the utility functions
	 * and the associated BinaryFileInfo object.
	 * <p>
	 * Note, that one thread ID may have several assigned binary files in the map, however
	 * we just pass the first one from the set, due to avoid heavy modification in the 
	 * utility methods. Marked, as a pending task.
	 * 
	 * @param  threadID  the thread ID that we want to fetch the meta information for
	 * @return  HashMap<String, String>  the meta information for the thread
	 * */
	public HashMap<String, String> getMeta(int threadID) {
		return binaryFileInfo.getMetaInfo().get(assignmentMap.get(threadID).first());
	}

	/**
	 * This function passes the full path of the file, that is assigned to the thread.
	 * <p>
	 * Note, that one thread ID may have several assigned binary files in the map, however
	 * we just pass the first one from the set, due to avoid heavy modification in the 
	 * utility methods. Marked, as a pending task.
	 * 
	 * @param  threadID  the ID that we want to fetch the meta information for
	 * @return  String  the full path to the assigned file
	 * */
	public String getFilePath(int threadID) {
		return assignedPathMap.get(threadID).first();
	}

}
