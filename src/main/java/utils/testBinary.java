package utils;

public class testBinary {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		BinaryFileInfo binaryInfo = BinaryFileInfo.getInstance("/home/cb/CMS/Projects/PaGeS/fb6fd61e48a708d56383/out");
		System.out.println("Hey ho..");
		//System.out.println(binaryInfo.getMetaFileList().toString());
		System.out.println(binaryInfo.getRandomMeta().toString());
		System.out.println(binaryInfo.getBinaryFileAbsolutePath("rbinary-HUGE-1.bin.chunks"));
		System.out.println(binaryInfo.getNumOfFiles());
		System.out.println(binaryInfo.getOriginalFilePathList().toString());
		System.out.println(binaryInfo.getXthBinaryID(0));
		System.out.println(binaryInfo.getXthBinaryID(1));
		System.out.println(binaryInfo.getXthBinaryID(2));
		System.out.println(binaryInfo.getBinaryFileAbsolutePath("rbinary-HUGE-1.bin.chunks"));
		System.out.println(binaryInfo.getOriginalFilePathList().get("rbinary-HUGE-1.bin"));
		
	}

	

}
