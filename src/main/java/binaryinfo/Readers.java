package binaryinfo;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64OutputStream;

public class Readers {

	public static class BinaryReader {

		public static List<ByteArrayOutputStream> readChunks(TreeMap<String, String> cPathList) {
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
		
		/** Read the given binary file, and return its contents as a byte array.
		 * @param  inputFileName  the binary file name of the file to read in
		 * @return  ByteArrayOutputStream  the file's content
		 * */
		public static ByteArrayOutputStream read(String fullFilePath) {
			ByteArrayOutputStream bosr = new ByteArrayOutputStream();
			File file = new File(fullFilePath);
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
	}

	public static class Base64Reader {
		/** Read the given binary file as Base64 string.
		 * @param  inputFileName  the binary file name of the file to read in
		 * @return  ByteArrayOutputStream  the file's content as Base64
		 * */
		public static ByteArrayOutputStream read(String inputFileName) {
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

}
