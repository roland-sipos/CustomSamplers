package utils;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.jmeter.samplers.SampleResult;

import com.mongodb.ServerAddress;

import binaryinfo.BinaryFileInfo;
import binaryinfo.Readers;
import binaryinfo.Readers.CustomReader;

public class tesBinary {

	public static class Foo {
		public static void classMethod() {
			System.out.println("classMethod() in Foo");
		}

		public void instanceMethod() {
			System.out.println("instanceMethod() in Foo");
		}
	}

	public static class Bar extends Foo {
		public static void classMethod() {
			System.out.println("classMethod() in Bar");
		}

		public void instanceMethod() {
			System.out.println("instanceMethod() in Bar");
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(Thread.currentThread().getId());

		System.out.println(" ------------ ");
		Foo f = new Bar();
		f.instanceMethod();
		f.classMethod();
		Bar.classMethod();
		System.out.println(" ------------ ");

		System.out.println(new String("rs/").concat("abc.cern.ch").concat(":27019"));

		Map<Integer, String> myMap = new TreeMap<Integer, String>();
		myMap.put(1, "blfsdf");
		myMap.put(2, "dsfdsg");
		myMap.put(3, "blfsdf");
		myMap.put(10, "yyyyyyyyyy");
		myMap.put(12, "fdshfjzuk");
		myMap.put(25, "blfsdf");
		myMap.put(26, "blfsdf");
		myMap.put(250, "dsgtrfjztgkghfds");
		myMap.put(100, "fdsghfjgfbvvdcsxxxxdfgdsfgdsfgd");
		myMap.put(111, "a");
		myMap.put(1000, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");

		for (Map.Entry<Integer, String> entry : myMap.entrySet()) {
			System.out.println("Key = " + entry.getKey() + ", Value = "
					+ entry.getValue());
		}

		List<String> list = new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");
		System.out.println("As array:" + list.toArray().toString());
		System.out.println("As string: " + list.toString());

		String arrayAsString = list.toArray().toString();
		List<String> list2 = new ArrayList<String>();
		list2.add(arrayAsString);

		/*
		 * Assignment assign = null; try { assign = new Assignment(
		 * "/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml"
		 * , "mixed", 10, BinaryFileInfo.getInstance("/testdata2/")); } catch
		 * (CustomSamplersException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * Assignment assign2 = null; try { assign2 = new Assignment(
		 * "/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml"
		 * , "mixed", 30, BinaryFileInfo.getInstance("/testdata/")); } catch
		 * (CustomSamplersException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * Assignment assign3 = null; try { assign3 = new Assignment(
		 * "/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml"
		 * , "mixed", 10, BinaryFileInfo.getInstance("/testdata/")); } catch
		 * (CustomSamplersException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * 
		 * System.out.println(assign3.getAssignments().toString());
		 * HashMap<String, String> res = assign3.getMeta(1); Iterator it =
		 * res.entrySet().iterator(); while (it.hasNext()) { Map.Entry pairs =
		 * (Map.Entry)it.next(); System.out.println(pairs.getKey() + " = " +
		 * pairs.getValue()); }
		 * System.out.println(assign3.getMeta(1).toString());
		 * 
		 * System.out.println(assign3.getBinaryFileInfo().toString());
		 * System.out.println(assign2.getBinaryFileInfo().toString());
		 * System.out.println(assign.getBinaryFileInfo().toString());
		 * 
		 * System.out.println(assign3.toString() + " -> " +
		 * assign3.getBinaryFileInfo().getInputLocation());
		 * System.out.println(assign.toString() + " -> " +
		 * assign.getBinaryFileInfo().getInputLocation());
		 * 
		 * System.out.println(BinaryFileInfo.getInstance("/testdata/").
		 * getInputLocation());
		 * System.out.println(BinaryFileInfo.getInstance("/testdata2/"
		 * ).getInputLocation());
		 * 
		 * System.out.println("\n");
		 * System.out.println(assign.getAssignments().toString());
		 */

		BinaryFileInfo binInfo = BinaryFileInfo
				.getInstance("/mnt/BACKUP/TESTDATA_GHOSTHAWK/out/");
		CustomReader binReader = new Readers.BinaryReader();
		ByteArrayOutputStream payload = binReader.read(binInfo
				.getAbsolutePathFor("rbinary-SMALL-129.bin"));
		System.out.println(" --- Binary byte[] --- ");
		System.out.println("Size [Bytes]: " + payload.size());
		System.out.println("Size [MB]: "
				+ String.valueOf(((payload.size() / 1024.0) / 1024.0)) + "MB");

		CustomReader b64Reader = new Readers.Base64Reader();
		ByteArrayOutputStream b64payload = b64Reader.read(binInfo
				.getAbsolutePathFor("rbinary-SMALL-129.bin"));
		System.out.println(" --- Base64 byte[] --- ");
		System.out.println("Size [Bytes]: " + b64payload.size());
		System.out.println("Size [MB]: "
				+ String.valueOf(((b64payload.size() / 1024.0) / 1024.0))
				+ "MB");

		SampleResult res = CustomSamplerUtils
				.getInitialSampleResult("someCrap");
		res.setBytes(payload.size());
		System.out.println(res.toString());
		System.out.println(res.getBytes());

		/*
		 * BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance( "/testdata/",
		 * "/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml"
		 * );
		 * 
		 * ArrayList<HashMap<String, String> > cucc =
		 * binaryInfo.getAssignedMeta(1); for (final Iterator<HashMap<String,
		 * String> > it = cucc.iterator(); it.hasNext(); ) { HashMap<String,
		 * String> actual = it.next(); if (actual != null) {
		 * System.out.println(actual.toString()); } else {
		 * System.out.println("it's null"); }
		 * 
		 * System.out.println(); }
		 * System.out.println(binaryInfo.getAssignedMeta(1).toString());
		 */

		/*
		 * System.out.println("------"); ByteArrayOutputStream original =
		 * binaryInfo.read(binaryInfo.getAbsolutePathFor("rbinary-1.bin"));
		 * ByteArrayOutputStream base64 =
		 * binaryInfo.readAsBase64(binaryInfo.getAbsolutePathFor
		 * ("rbinary-1.bin")); for (int i = 0; i < 10; ++i) {
		 * System.out.print(original.toByteArray()[i] + " ");
		 * //System.out.println(base64.toByteArray()[i] + " "); }
		 * System.out.println(); System.out.println("------"); for (int i = 0; i
		 * < 10; ++i) { //System.out.print(original.toByteArray()[i] + " ");
		 * System.out.print(base64.toByteArray()[i] + " "); }
		 * System.out.println(); System.out.println("------");
		 * 
		 * String cProp = "false"; Boolean useChunks =
		 * cProp.equals(String.valueOf(Boolean.TRUE)) || cProp.equals("Bulk");
		 * System.out.println(useChunks);
		 * 
		 * String tablelist = "IOV, PAYLOAD, TAG"; String tables[] =
		 * tablelist.replaceAll("^[,\\s]+", "").split("[,\\s]+"); for (int i =
		 * 0; i < tables.length; ++i) { System.out.println(tables[i]); }
		 * System.out.println(Arrays.toString(tables));
		 */
		/*
		 * 
		 * String woof = "chunk-150.bin"; Integer woofInt =
		 * Integer.parseInt(woof.replaceAll("[\\D]", ""));
		 * System.out.println("The number of WOOF is: " + woofInt.toString());
		 * 
		 * String out = "ID:" + woofInt; System.out.println(out);
		 * 
		 * String str5 = "5"; Integer num5 = Integer.parseInt(str5); if (num5 ==
		 * 5) System.out.println("YAY COOL!");
		 * 
		 * 
		 * String testStr = "a_b_c_d_efdfsdfs_dsfsdfsd_fsdfsdgsd_ASAFS";
		 * String[] need = testStr.split("\\_");
		 * System.out.println(Arrays.toString(need));
		 */

	}

}
