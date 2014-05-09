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

	/**
	 * An interface that describes, that a CustomReader in the project needs to implement
	 * how a file, and a chunk of a file are read in the CustomSamplers project.
	 * */
	public interface CustomReader {
		public List<ByteArrayOutputStream> readChunks(TreeMap<String, String> cPathList);
		public ByteArrayOutputStream read(String fullFilePath);
	}

	/**
	 * The BinaryReader just reads the files into normal byte arrays.
	 * */
	public static class BinaryReader implements CustomReader {

		@Override
		public List<ByteArrayOutputStream> readChunks(TreeMap<String, String> cPathList) {
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
		@Override
		public ByteArrayOutputStream read(String fullFilePath) {
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

	public static class Base64Reader implements CustomReader {

		public List<ByteArrayOutputStream> readChunks(TreeMap<String, String> cPathList) {
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

		/** Read the given binary file as Base64 string.
		 * @param  inputFileName  the binary file name of the file to read in
		 * @return  ByteArrayOutputStream  the file's content as Base64
		 * */
		public ByteArrayOutputStream read(String inputFileName) {
			ByteArrayOutputStream res = null;
			try {
				int BUFFER_SIZE = 4096;
				byte[] buffer = new byte[BUFFER_SIZE];
				InputStream input = new FileInputStream(inputFileName);

				res = new ByteArrayOutputStream();

				/* public Base64OutputStream(OutputStream out, boolean doEncode,
				 * 		int lineLength, byte[] lineSeparator) */
				OutputStream output = new Base64OutputStream(res, true, 0, new byte[1]);
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
