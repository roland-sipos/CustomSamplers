package rsipos.customsamplers;

import java.util.Random;

import utils.BinaryFileInfo;

/**
 * Hello world!
 *
 */
public class App 
{
	private static String inputLocation = "/home/cb/INPUT";
    
    private static BinaryFileInfo binaryInfo; 
    
    public static void dump(Dog what) {
    	System.out.println("Inside: " + what.toString() + " value:" + what.getName());
    	what.setName("WOOF WOOF!");
    	System.out.println("Inside2: " + what.toString() + " value:" + what.getName());
    }
    
    public static void main( String[] args )
    {   
    	Dog fooDog = new Dog();
    	fooDog.setName("WOOT");
    	
    	System.out.println("Before: " + fooDog.toString() + " value:" + fooDog.getName());
    	dump(fooDog);
    	System.out.println("After: " + fooDog.toString() + " value:" + fooDog.getName());
    	
    	int counter = 0;
    	System.out.println(counter);
    	counter++;
    	System.out.println(counter);
   
    	Random rand = new Random();
    	int min = 0;
    	int max = 5;
    	
    	System.out.println("New sequence: ");
    	for (int i = 0; i < 40; ++i) {
    		int newR = rand.nextInt(max - min + 1) + min;
    		System.out.print(newR + " ");
    	}
    	
    	binaryInfo = BinaryFileInfo.getInstance(inputLocation);
    	
    	System.out.println("Input loc:" + binaryInfo.getInputLocation());
        
        System.out.println("Content of the maps:");
        System.out.println(" -> MetaFileList:");
        System.out.println(binaryInfo.getMetaFileList().toString());
        System.out.println(" -> MetaInfo:");
        System.out.println(binaryInfo.getMetaInfo().toString());
        System.out.println(" -> BinaryFileList:");
        System.out.println(binaryInfo.getBinaryFilePathList());
        System.out.println(" -> OriginalFileList:");
        System.out.println(binaryInfo.getOriginalFilePathList());
        
        //System.out.println(binaryInfo.getMe)
        
        
        /*String threadName = "Thread Group 1-11";
    	int threadId = CustomSamplerUtils.getThreadID(threadName);
    	System.out.println("THE ID IS: " + threadId);*/
    }

}
