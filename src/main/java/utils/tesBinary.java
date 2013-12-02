package utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import assignment.Assignment;
import binaryinfo.BinaryFileInfo;

public class tesBinary {
	
	public static class Test
	{
	    public String test()
	    {
	        try {
	            System.out.println("try");
	            throw new Exception();
	        } catch(Exception e) {
	            System.out.println("catch");
	            //return "return"; 
	        } finally {  
	            System.out.println("finally");
	            return "return in finally"; 
	        }
	    }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println(Thread.currentThread().getId());

		/*Assignment assign = null;
		try {
			 assign = new Assignment(
					"/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml",
					"mixed", 10, BinaryFileInfo.getInstance("/testdata2/"));
		} catch (CustomSamplersException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Assignment assign2 = null;
		try {
			 assign2 = new Assignment(
					"/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml",
					"mixed", 30, BinaryFileInfo.getInstance("/testdata/"));
		} catch (CustomSamplersException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Assignment assign3 = null;
		try {
			assign3 = new Assignment(
					"/home/cb/EclipseWorkspace/customsamplers/resources/assignments/example.xml",
					"mixed", 10, BinaryFileInfo.getInstance("/testdata/"));
		} catch (CustomSamplersException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		System.out.println(assign3.getAssignments().toString());
		HashMap<String, String> res = assign3.getMeta(1);
		Iterator it = res.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			System.out.println(pairs.getKey() + " = " + pairs.getValue());
		}
		System.out.println(assign3.getMeta(1).toString());

		System.out.println(assign3.getBinaryFileInfo().toString());
		System.out.println(assign2.getBinaryFileInfo().toString());
		System.out.println(assign.getBinaryFileInfo().toString());
		
		System.out.println(assign3.toString() + " -> " + assign3.getBinaryFileInfo().getInputLocation());
		System.out.println(assign.toString() + " -> " + assign.getBinaryFileInfo().getInputLocation());

		System.out.println(BinaryFileInfo.getInstance("/testdata/").getInputLocation());
		System.out.println(BinaryFileInfo.getInstance("/testdata2/").getInputLocation());

		System.out.println("\n");
		System.out.println(assign.getAssignments().toString());*/
		
		Test test = new Test();
		System.out.println(test.test());
		
		/*BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance(
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
		System.out.println(binaryInfo.getAssignedMeta(1).toString());*/
		
		
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
