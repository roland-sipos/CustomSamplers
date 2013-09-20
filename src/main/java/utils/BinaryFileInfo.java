package utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class BinaryFileInfo {

	private static BinaryFileInfo instance = null;

	private static String location;
	private static boolean isAssignAvailable;
	private static String assignmentFile;
	private static int numOfFiles;

	private static TreeMap<String, HashMap<String, String> > metaInfo;
	private static TreeMap<String, String> filePathList;
	private static TreeMap<String, TreeMap<String, String> > chunkPathList;

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

	public String getAbsolutePathFor(String fileName) {
		return filePathList.get(fileName);
	}

	public String getAbsolutePathForChunk(String fileName, String chunkName) {
		return chunkPathList.get(fileName).get(chunkName);
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
			createAssignmentTable();
			isAssignAvailable = true;
		} else {
			assignmentFile = "";
			isAssignAvailable = false;
		}
		numOfFiles = 0;
		filePathList = new TreeMap<String, String>();
		chunkPathList = new TreeMap<String, TreeMap<String, String> >();
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

	private void createAssignmentTable() {
		
		try {
			File asFile = new File(assignmentFile);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(asFile);
			doc.getDocumentElement().normalize();
			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

			NodeList nList = doc.getElementsByTagName("payload");
			System.out.println("----------------------------");
		 
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					System.out.println("Payload name : " + eElement.getAttribute("name"));
					System.out.println("threadIDList : " + eElement.getElementsByTagName("threadIDList").item(0).getTextContent());
					Element subE = (Element) eElement.getElementsByTagName("threadIDRange").item(0);
					System.out.println("threadIDRange FROM: " + subE.getAttribute("from"));
					System.out.println("threadIDRange TO: " + subE.getAttribute("to"));
					//System.out.println("Nick Name : " + eElement.getElementsByTagName("nickname").item(0).getTextContent());
					//System.out.println("Salary : " + eElement.getElementsByTagName("salary").item(0).getTextContent());
		 
				}
			}
			
			/*BufferedReader in = new BufferedReader(new FileReader(asFile));
			String line = "";
			HashMap<String, String> metaMapForBinary = new HashMap<String, String>();
			while ((line = in.readLine()) != null) {
				String cols[] = line.split(" ");
				metaMapForBinary.put(cols[0], cols[1]);
			}
			in.close();*/
		} catch (Exception e) {
			System.out.println(e.toString());
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
		TreeMap<String, String> list = new TreeMap<String, String>();
		File[] chunks = getChunkFiles(dir);
		for (int i = 0; i < chunks.length; ++i) {
			list.put(chunks[i].getName(), chunks[i].getAbsolutePath());
		}
		chunkPathList.put(binaryName, list);
	}


	public HashMap<String, String> getRandomMeta() {
		// TODO: Create the random generation customizable.
		Random random = new Random();
		// Get a random Binary ID.
		Object[] fileNameArray = getFilePathList().keySet().toArray();
		String binaryID = (String) fileNameArray[random.nextInt(fileNameArray.length)];
		return getMetaInfo().get(binaryID);
	}

	public HashMap<String, String> getAssignedMeta(int type) {
		//TODO: Get an assigned meta information based on input file or thread group
		return null;
	}

	public String getXthFileName(int x) {
		Object[] fileNameArray = getFilePathList().keySet().toArray();
		return (String) fileNameArray[x-1];
	}

	/** Read the given binary file, and return its contents as a byte array.*/ 
	public byte[] read(String aInputFileName) {
		//System.out.println("Reading in binary file named : " + aInputFileName);
		File file = new File(aInputFileName);
		//System.out.println("File size: " + file.length());
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
				/*
				 * the above style is a bit tricky: it places bytes into the 'result' array; 
				 * 'result' is an output parameter;
				 * the while loop usually has a single iteration only.
				 */
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
		return result;
	}

}
