package utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.jmeter.samplers.SampleResult;

public class tesBinary {
	
	private static ByteArrayOutputStream mergeByteArrayList(List<ByteArrayOutputStream> list) {
		ByteArrayOutputStream resOs = new ByteArrayOutputStream();
		try {
			Iterator<ByteArrayOutputStream> lIt = list.iterator(); 
			while (lIt.hasNext()) {
				ByteArrayOutputStream ba = lIt.next();
				resOs.write(ba.toByteArray());
			}
		} catch (IOException e){
			System.out.println("YAY! Fucked up!");
		}	
		return resOs;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance(
				"/home/cb/CMS/Projects/PaGeS/fb6fd61e48a708d56383/out",
				"/home/cb/EclipseWorkspace/customsamplers/resources/assignments/basic.xml");

		System.out.println("------");
		ByteArrayOutputStream original = binaryInfo.read(binaryInfo.getAbsolutePathFor("rbinary-1.bin"));
		ByteArrayOutputStream base64 = binaryInfo.readAsBase64(binaryInfo.getAbsolutePathFor("rbinary-1.bin"));
		for (int i = 0; i < 10; ++i) {
			System.out.print(original.toByteArray()[i] + " ");
			//System.out.println(base64.toByteArray()[i] + " ");
		}
		System.out.println();
		System.out.println("------");
		for (int i = 0; i < 10; ++i) {
			//System.out.print(original.toByteArray()[i] + " ");
			System.out.print(base64.toByteArray()[i] + " ");
		}
		System.out.println();
		System.out.println("------");
		/*
		
		String woof = "chunk-150.bin";
		Integer woofInt = Integer.parseInt(woof.replaceAll("[\\D]", ""));
		System.out.println("The number of WOOF is: " + woofInt.toString());
		
		String out = "ID:" + woofInt;
		System.out.println(out);
		
		String str5 = "5";
		Integer num5 = Integer.parseInt(str5);
		if (num5 == 5) System.out.println("YAY COOL!");
		
		
		String testStr = "a_b_c_d_efdfsdfs_dsfsdfsd_fsdfsdgsd_ASAFS";
		String[] need = testStr.split("\\_");
		System.out.println(Arrays.toString(need));
		*/
		
	}

}
