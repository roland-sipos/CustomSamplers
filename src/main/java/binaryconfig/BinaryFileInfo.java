package binaryconfig;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64OutputStream;

public class BinaryFileInfo {

	private static BinaryFileInfo instance = null;

	private static String location;
	private static boolean isAssignAvailable;
	private static String assignmentFile;
	private static int numOfFiles;

	private static TreeMap<String, HashMap<String, String> > metaInfo;
	private static TreeMap<String, String> filePathList;
	private static TreeMap<String, TreeMap<String, String> > chunkPathList;
	private static TreeMap<String, TreeMap<String, Integer> > chunkIDList;

	private static HashMap<Integer, SortedSet<String>> assignMap;

	public String getInputLocation() {
		return location;
	}

	public String getAssignmentFile() {
		return assignmentFile;
	}
	
	public boolean isAssignAvailable() {
		return isAssignAvailable;
	}
	
	public int getNumOfFiles() {
		return numOfFiles;
	}

	public TreeMap<String, HashMap<String, String> > getMetaInfo() {
		return metaInfo;
	}

	public TreeMap<String, String> getFilePathList() {
		return filePathList;
	}

	public TreeMap<String, TreeMap<String, String> > getChunkPathList() {
		return chunkPathList;
	}

	public TreeMap<String, TreeMap<String, Integer> > getChunkIDList() {
		return chunkIDList;
	}

	public String getAbsolutePathFor(String fileName) {
		return filePathList.get(fileName);
	}

	public String getAbsolutePathForChunk(String fileName, String chunkName) {
		return chunkPathList.get(fileName).get(chunkName);
	}

	public Integer getIDForChunk(String fileName, String chunkName) {
		return chunkIDList.get(fileName).get(chunkName);
	}

	public String getPathForStreamerInfo(String fileName) {
		return filePathList.get(fileName) + ".chunks/STREAMER_INFO.bin";
	}

	public static BinaryFileInfo getInstance(String location, String assignFile) {
		if (instance == null) {
			instance = new BinaryFileInfo(location, assignFile);
		}
		return instance;
	}

	protected BinaryFileInfo(String loc, String assignFile) {
		location = loc;
		if (assignFile != "") {
			assignmentFile = assignFile;
			assignMap = AssignmentXMLParser.parse(assignmentFile);
			isAssignAvailable = true;
		} else {
			assignmentFile = "";
			isAssignAvailable = false;
		}
		numOfFiles = 0;
		filePathList = new TreeMap<String, String>();
		chunkPathList = new TreeMap<String, TreeMap<String, String> >();
		chunkIDList = new TreeMap<String, TreeMap<String, Integer> >();
		metaInfo = new TreeMap<String, HashMap<String, String> >();

		File[] locFolder = new File(location).listFiles();
		for (File sub : locFolder) {
			if (sub.isFile()) {
				numOfFiles++;
				// Store the path in filePathList
				String binaryName = sub.getName();
				filePathList.put(sub.getName(), sub.getAbsolutePath());
				//There must be a sub-dir for the file with .chunks suffix:
				File dir = new File(sub.getAbsolutePath() + ".chunks");
				if (dir.isDirectory()) {
					try {
						readMetaForFile(binaryName, dir);
						readChunkPathList(binaryName, dir);
					} catch (Exception ex) {
						System.out.println(ex.toString());
					}
				}
			} else {
				// IGNORE ANYTHING ELSE, dirs already processed.
			}
		}
	}

	/*private String getMetaFilePath(File folder) {
		File[] metaFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.toLowerCase().endsWith(".info");
			}
		});
		return metaFiles[0].getAbsolutePath();
	}*/ // Clerver mode to get any .info file, but simply use a strict form is faster.
	private void readMetaForFile(String binaryName, File dir) throws FileNotFoundException, IOException {
		File metaFile = new File(dir.getAbsolutePath() + "/META-" + binaryName + ".info");
		BufferedReader in = new BufferedReader(new FileReader(metaFile));
		String line = "";
		HashMap<String, String> metaMapForBinary = new HashMap<String, String>();
		while ((line = in.readLine()) != null) {
			String cols[] = line.split(" ");
			metaMapForBinary.put(cols[0], cols[1]);
		}
		metaMapForBinary.put("id", binaryName);
		metaInfo.put(binaryName, metaMapForBinary);
		in.close();
	}

	private File[] getChunkFiles(File folder) {
		File[] chunkFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.toLowerCase().endsWith(".bin") && !name.startsWith("STREAMER_INFO");
			}
		});
		return chunkFiles;
	}
	private void readChunkPathList(String binaryName, File dir) {
		TreeMap<String, String> namelist = new TreeMap<String, String>();
		TreeMap<String, Integer> idlist = new TreeMap<String, Integer>();
		File[] chunks = getChunkFiles(dir);
		for (int i = 0; i < chunks.length; ++i) {
			String chunkName = chunks[i].getName();
			namelist.put(chunkName, chunks[i].getAbsolutePath());
			idlist.put(chunkName, Integer.parseInt(chunkName.replaceAll("[\\D]", "")));
		}
		chunkPathList.put(binaryName, namelist);
		chunkIDList.put(binaryName, idlist);
	}


	public HashMap<String, String> getRandomMeta() {
		// TODO: Create the random generation customizable.
		Random random = new Random();
		// Get a random Binary ID.
		Object[] fileNameArray = getFilePathList().keySet().toArray();
		String binaryID = (String) fileNameArray[random.nextInt(fileNameArray.length)];
		return getMetaInfo().get(binaryID);
	}

	public ArrayList<HashMap<String, String> > getAssignedMeta(Integer threadID) {
		//TODO: Get an assigned meta information based on input file or thread group
		ArrayList<HashMap<String, String> > result = new ArrayList<HashMap<String, String> >();
		SortedSet<String> fileSet = assignMap.get(threadID);
		for (final Iterator<String> it = fileSet.iterator(); it.hasNext(); ) {
			result.add(getMetaInfo().get(it.next()));
		}
		return result;
	}

	public String getXthFileName(int x) {
		Object[] fileNameArray = getFilePathList().keySet().toArray();
		return (String) fileNameArray[x-1];
	}

	public List<ByteArrayOutputStream> readChunksFor(String binaryName) {
		TreeMap<String, String> cPathList = chunkPathList.get(binaryName);
		int size = cPathList.entrySet().size();
		
		List<ByteArrayOutputStream> res = new ArrayList<ByteArrayOutputStream>(size);
		for (int i = 0; i < size; ++i) {
			res.add(i, null);
		}
		for (Map.Entry<String, String> it : cPathList.entrySet()) {
			int id = Integer.parseInt(it.getKey().replaceAll("[\\D]", ""));
			res.set(id-1, read(it.getValue()));
		}
		return res;
	}
	
	/** Read the given binary file, and return its contents as a byte array.*/ 
	public ByteArrayOutputStream read(String aInputFileName) {
		ByteArrayOutputStream bosr = new ByteArrayOutputStream();
		File file = new File(aInputFileName);
		byte[] result = new byte[(int)file.length()];
		try {
			InputStream input = null;
			try {
				int totalBytesRead = 0;
				input = new BufferedInputStream(new FileInputStream(file));
				while(totalBytesRead < result.length){
					int bytesRemaining = result.length - totalBytesRead;
					//input.read() returns -1, 0, or more :
					int bytesRead = input.read(result, totalBytesRead, bytesRemaining); 
					if (bytesRead > 0){
						totalBytesRead = totalBytesRead + bytesRead;
					}
				}
				bosr.write(result);
			}
			finally {
				input.close();
			}
		}
		catch (FileNotFoundException ex) {
			System.out.println("File not found.");
		}
		catch (IOException ex) {
			System.out.println(ex.toString());
		}
		return bosr;
	}

	/** Read the given binary file, and return its contents as a byte array.*/ 
	public ByteArrayOutputStream readAsBase64(String inputFileName) {
		ByteArrayOutputStream res = null;
		try {
			int BUFFER_SIZE = 4096;
			byte[] buffer = new byte[BUFFER_SIZE];
			InputStream input = new FileInputStream(inputFileName);

			res = new ByteArrayOutputStream();
			OutputStream output = new Base64OutputStream(res);
			//a.writeTo(result);
			int n = input.read(buffer, 0, BUFFER_SIZE);
			while (n >= 0) {
				output.write(buffer, 0, n);
				n = input.read(buffer, 0, BUFFER_SIZE);
			}
			input.close();
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

}
