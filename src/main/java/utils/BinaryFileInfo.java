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

public class BinaryFileInfo {

	private static BinaryFileInfo instance = null;
	
	private static String location;
	// TreeMap<"binaryId", "metaFilePathList">
	private static TreeMap<String, String> metaFileList;
	// TreeMap<"binaryId", TreeMap<"chunkId", "hash"> >
	private static TreeMap<String, TreeMap<String, String> > metaInfo;
    // TreeMap<"binaryId", TreeMap<"chunkId", "path"> >
	private static TreeMap<String, TreeMap<String, String> > binaryFilePathList;
	// TreeMap<"binaryId", "path">
	private static TreeMap<String, String> originalFilePathList;
	
    public String getInputLocation() {
        return location;
    }
    public TreeMap<String, TreeMap<String, String> > getMetaInfo() {
        return metaInfo;
    }
    public TreeMap<String, String> getMetaFileList() {
        return metaFileList;
    }
    public TreeMap<String, TreeMap<String, String> > getBinaryFilePathList() {
        return binaryFilePathList;
    }
    public TreeMap<String, String> getOriginalFilePathList() {
    	return originalFilePathList;
    }
    
    public static BinaryFileInfo getInstance(String location) {
    	if (instance == null) {
    		instance = new BinaryFileInfo(location);
    	}
    	return instance;
    }
    
    protected BinaryFileInfo(String loc) {
        location = loc; //baseDir.concat("BIGrbinary-"+ID+".bin.chunks/");
        metaInfo = new TreeMap<String, TreeMap<String, String> >();
        metaFileList = new TreeMap<String, String>();
        binaryFilePathList = new TreeMap<String, TreeMap<String, String> >();
        originalFilePathList = new TreeMap<String, String>();

        File[] locFolder = new File(location).listFiles();
        for (File sub : locFolder) {
        	if (sub.isDirectory()) {
        		try {
        			prepareMETA(sub);
        			String metaFilePath = getMetaFilePath(sub);
        			metaFileList.put(sub.getName(), metaFilePath);
        			TreeMap<String, String> binaryFiles = getBinaryFiles(sub);
        			binaryFilePathList.put(sub.getName(), binaryFiles);
        			
        		} catch (Exception ex) {
        			System.out.println(ex.toString());
        		}
        	} else if (sub.isFile()) { // It's an original, BIG file!
        		originalFilePathList.put(sub.getName(), sub.getAbsolutePath());
        	} // else: not usable
        }
    }
    
    public String getBinaryFileAbsolutePath(String fileName) {
        return binaryFilePathList.get(fileName).toString();
    }
    
    private void prepareMETA(File folder) throws FileNotFoundException, IOException {
        String binaryName = folder.getName();
        File metaFile = new File(getMetaFilePath(folder));
        BufferedReader in = new BufferedReader(new FileReader(metaFile));
        String line = "";
        TreeMap<String, String> metaMapForBinary = new TreeMap<String, String>();
        while ((line = in.readLine()) != null) {
            String cols[] = line.split(" ");
            metaMapForBinary.put(cols[0], cols[1]);
        }
        metaInfo.put(binaryName, metaMapForBinary);
        in.close();
    }
    
    private String getMetaFilePath(File folder) {
        File[] metaFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".info");
            }
        });
        return metaFiles[0].getAbsolutePath();
    }
    private TreeMap<String, String> getBinaryFiles(File folder) {
        File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".bin");
            }
        });
    	TreeMap<String, String> filePathList = new TreeMap<String, String>();
        for (int i = 0; i < files.length; ++i) {
        	filePathList.put(files[i].getName(), files[i].getAbsolutePath());
        }
        return filePathList;
    }
 
    public HashMap<String, String> getHashesForIDs(String binaryID, String chunkID) {
    	HashMap<String, String> retMap = new HashMap<String, String>();
    	retMap.put("original", getMetaInfo().get(binaryID).get("ORIGINAL"));
    	retMap.put("chunk", getMetaInfo().get(binaryID).get(chunkID));
    	return retMap;
    }
    
    public HashMap<String, String> getRandomHashesAndIDs() {
    	Random random = new Random();
    	HashMap<String, String> retMap = new HashMap<String, String>();
    	// Get a random Binary ID.
		Object[] binaryIDsArray = getMetaFileList().keySet().toArray();
		String binaryID = (String) binaryIDsArray[random.nextInt(binaryIDsArray.length)];  
		
		// Get a random ChunkID from the Binary.
		Object[] chunkIDsArray = getMetaInfo().get(binaryID).keySet().toArray();
		String chunkID = (String) chunkIDsArray[random.nextInt(chunkIDsArray.length - 2 + 1) + 2]; // random between range 2:MIN (first two entry is ORIGINAL AND HASH)
		retMap.put("originalID", binaryID);
		retMap.put("chunkID", chunkID);
		
		// Get the hashes for both the original binary, and it's sub-chunk.
		String originalHash = getMetaInfo().get(binaryID).get("ORIGINAL");
		String chunkHash = getMetaInfo().get(binaryID).get(chunkID);
		
		retMap.put("original", originalHash);
		retMap.put("chunk", chunkHash);
		return retMap;
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
                    the above style is a bit tricky: it places bytes into the 'result' array; 
                    'result' is an output parameter;
                    the while loop usually has a single iteration only.
                */
                //System.out.println("Num bytes read: " + totalBytesRead);
            }
            finally {
                //System.out.println("Closing input stream.");
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
