package utils;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import binaryconfig.BinaryFileInfo;

public class tesBinary {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance(
				"/testdata/",
				"/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml");

		ArrayList<HashMap<String, String> > cucc = binaryInfo.getAssignedMeta(1);
		for (final Iterator<HashMap<String, String> > it = cucc.iterator(); it.hasNext(); ) {
			HashMap<String, String> actual = it.next();
			if (actual != null) {
				System.out.println(actual.toString());
			} else {
				System.out.println("it's null");
			}
			
			System.out.println();
		}
		System.out.println(binaryInfo.getAssignedMeta(1).toString());
		
		
		/*System.out.println("------");
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
		
		String cProp = "false";
		Boolean useChunks = cProp.equals(String.valueOf(Boolean.TRUE)) || cProp.equals("Bulk");
		System.out.println(useChunks);
		
		String tablelist = "IOV, PAYLOAD, TAG";
		String tables[] = tablelist.replaceAll("^[,\\s]+", "").split("[,\\s]+");
		for (int i = 0; i < tables.length; ++i) {
			System.out.println(tables[i]);
		}
		System.out.println(Arrays.toString(tables));
		*/
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
