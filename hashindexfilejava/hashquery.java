import java.lang.Integer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.io.File;
import java.util.Arrays;
import java.lang.Long;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class hashquery
{
    public static void main(String[] args)
    {
	int pagesize = 0;
	String textQuery = "";
	if(args.length != 2)
	    {
		System.out.println("Incorrect number of Arguments");
		System.exit(0);
	    }
        boolean numFound = false;
	try
	    {
		pagesize = Integer.parseInt(args[0]);
		textQuery = args[1];
		numFound = true;
	    }
	catch(NumberFormatException e)  
	    {  
		numFound = false;  
	    } 
	if(!numFound)
	    {
		try
		    {
			pagesize = Integer.parseInt(args[1]);
			textQuery = args[0];
		    }
		catch(NumberFormatException e)
		    {
			System.out.println("Non-numerical pagesize entered");
			System.exit(0);
		    }
	    }
	if(textQuery.equals(""))
	    {
		System.exit(0);
	    }
	// file to read indexs from
	String hashfile = "hash." + pagesize;
	// file to read the record from
	String heapfile = "heap." + pagesize;
	// timing the search
	long startTime = System.currentTimeMillis();
	long[] numbers = QueryFile(hashfile, heapfile, pagesize, textQuery);
	long endTime = System.currentTimeMillis();	
        long duration = (endTime - startTime);
        System.out.println("Time taken in milliseconds: " + duration);
        System.out.println("Number of records searched: " + numbers[0]);
        System.out.println("Number of buckets searched: " + numbers[1]);
	System.out.println("Number of records found:" + numbers[2]);

    }
    public static long[] QueryFile(String hashfile, String heapfile, int pagesize, String textQuery)
    {
	// Size of Bucket in Bytes
	final int bucketByteSize = 44004;
	//int bucketByteSize = 84;
	final int numberOfIndexes = bucketByteSize / 12;
	final int numberOfBuckets = 1024;
	// counter for end of program
	int numOfBuckets = 0;
	long numOfRecords = 0;
	long numOfFoundRecords = 0;
	// value to search for
	int queryValue = textQuery.hashCode();
	// bucket index is most likely to be in
	int bucketIndex = Math.abs(queryValue % numberOfBuckets);
	RandomAccessFile inputhash = null;
	FileInputStream inputheap = null;
	try
	    {
		inputhash = new RandomAccessFile(new File(hashfile), "rw");
		inputheap = new FileInputStream(heapfile);
		// Length of heap read (no maximum record size so len is used)
		int len = pagesize;
		// how far into the page the record is 
		long offset = 0;
		// where the page from the heap file will be read into
		byte[] buffer = new byte[len]; 
		// overall offset of the file from the start point;
		// stopping point for hashquery (if bucketIndex == bucketIndex at the end of the loop stop searching)
		int startIndex = bucketIndex;
		boolean backToStart = false;
		boolean recordFound = false;
		// While the end of all pages has not been found
		while(!backToStart && !recordFound)
		    {
			numOfBuckets++;
			// total offset, how much the random access file should skip
			offset = bucketIndex * bucketByteSize;	
			// move file pointer to start of the bucket
			inputhash.seek(offset);
			int indexNum = 0;
			while(indexNum < numberOfIndexes && !recordFound)
			    {
				indexNum++;
				numOfRecords++;
				// Get the hash index to compare to the query value
				byte[] valueSlice = new byte[4];
				inputhash.read(valueSlice);
				int value = byteToInt(valueSlice);
				// Get the offset from the start of the heap file
				byte[] offsetSlice = new byte[8];
				inputhash.read(offsetSlice);
				long fileOffset = byteToLong(offsetSlice);
				if(value == queryValue)
				    {
					// skip fileOffset amount of bytes
					inputheap.skip(fileOffset);
					// read up to pagesize number of bytes
					inputheap.read(buffer);
					// Because this is a non-fixed length database, each record must be read in its entirety before creating the hashinex
					// As all the string fields (BNNAME, BNSTATUS etc.) are variable (are not automatically set to the maximum length)
					    
					// Read BUSINESS NAMES from file
					int buffset = 0;
					byte[] slice1 = Arrays.copyOfRange(buffer, buffset, buffset + 14); 
					String regname = new String(slice1);
					buffset = buffset + 14;
					// Read namelength to file
					byte[] slice2 = Arrays.copyOfRange(buffer, buffset, buffset + 4);
					int namelength = byteToInt(slice2);
					buffset = buffset + 4;
					// read BN_NAMES from file
					byte[] slice3 = Arrays.copyOfRange(buffer, buffset, buffset + namelength); 
					String bnname = new String(slice3);
					buffset = buffset + namelength;
					// read status length from file
					byte[] slice4 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
					int statuslength = byteToInt(slice4);
					buffset = buffset + 4;
					// read bnstatus from file
					byte[] slice5 = Arrays.copyOfRange(buffer, buffset, buffset + statuslength); 
					String bnstatus = new String(slice5);
					buffset = buffset + statuslength;
					// read regdate length from file
					byte[] slice6 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
					int reglength = byteToInt(slice6);
					buffset = buffset + 4;
					// read regdt from file
					byte[] slice7 = Arrays.copyOfRange(buffer, buffset, buffset + reglength); 
					String bnregdt = new String(slice7);
					buffset = buffset + reglength;
					// read cancel date length from file
					byte[] slice8 = Arrays.copyOfRange(buffer, buffset, buffset + 4);  
					int cancellength = byteToInt(slice8);
					buffset = buffset + 4;
					// read bnstatus from file
					byte[] slice9 = Arrays.copyOfRange(buffer, buffset, buffset + cancellength); 
					String bncanceldt = new String(slice9);
					buffset = buffset + cancellength;
					// read renew length from file
					byte[] slice10 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
					int renewlength = byteToInt(slice10);
					buffset = buffset + 4;
					// read bnstatus from file
					byte[] slice11 = Arrays.copyOfRange(buffer, buffset, buffset + renewlength); 
					String bnrenewdt = new String(slice11);
					buffset = buffset + renewlength;
					// read status length from file
					byte[] slice12 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
					int statenumlength = byteToInt(slice12);
					buffset = buffset + 4;
					// read bnstatus from file
					byte[] slice13 = Arrays.copyOfRange(buffer, buffset, buffset + statenumlength); 
					String bnstatenum = new String(slice13);
					buffset = buffset + statenumlength;
					// read status length from file
					byte[] slice14 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
					int statereglength = byteToInt(slice14);
					buffset = buffset + 4;
					// read bnstatus from file
					byte[] slice15 = Arrays.copyOfRange(buffer, buffset, buffset + statereglength); 
					String bnstatereg = new String(slice15);
					buffset = buffset + statereglength;
					// read status length from file
					byte[] slice16 = Arrays.copyOfRange(buffer, buffset, buffset + 8); 
					long bnabn = byteToLong(slice16);
					buffset = buffset + 8;
					// exit out of loop
					recordFound = true;
					numOfFoundRecords++;
					// print out record 
					System.out.print("BN_REG_NAME: " + regname + "\nBN_NAME: " + bnname + "\nBN_STATUS: " + bnstatus + "\nBN_REG_DATE: ");
					System.out.print(bnregdt + "\nBN_CANCEL_DT: " + bncanceldt + "\nBN_RENEW_DT: " + bnrenewdt + "\nBN_STATE_NUM: ");
					System.out.print(bnstatenum + "\nBN_STATE_REG: " + bnstatereg);
					if(bnabn != -1)
					    {
						System.out.println("\nBN_ABN: " + bnabn);
					    }
					else
					    {
						System.out.println("\nBN_ABN: ");
					    }
				    }
			    }
			// Reached end of the bucket, increase index by one, if at 1024 set to 0
			bucketIndex = (bucketIndex + 1) % numberOfBuckets;
			if(bucketIndex  == startIndex )
			    {
				// Stop searching
				backToStart = true;
			    }
		    }
	    }
	catch (FileNotFoundException e)
	    {
		System.out.println("File Not Found");    
	    }
	catch (IOException e)
	    {
		e.printStackTrace();
	    }
	finally
	    {
		// close any input streams
		if(inputhash != null)
		    {
			try
			    {
				inputhash.close();
			    }
			catch (IOException ex)
			    {
				    
			    }
		    }
		if(inputheap != null)
		    {
			try
			    {
				inputheap.close();
			    }
			catch (IOException ex)
			    {
				    
			    }
		    }
		// return statistics
		long[] numbers = {numOfRecords, numOfBuckets, numOfFoundRecords};
		return numbers;
	    }
	}

    public static byte[] intToByte(int x)
    {
	byte[] blength = new byte[4];
	blength[0] = (byte) (x >> 24);
	blength[1] = (byte) (x >> 16);
	blength[2] = (byte) (x >> 8);
	blength[3] = (byte) (x);
	return blength;
    }
    public static byte[] longToByte(long x)
    {
	byte[] barray = new byte[8];
	barray[0] = (byte) (x >> 56);
	barray[1] = (byte) (x >> 48);
	barray[2] = (byte) (x >> 40);
	barray[3] = (byte) (x >> 32);
	barray[4] = (byte) (x >> 24);
	barray[5] = (byte) (x >> 16);
	barray[6] = (byte) (x >> 8);
	barray[7] = (byte) (x);
	return barray;
    }
    public static int byteToInt(byte[] slice)
    {
	int num = ((slice[0] & 0xFF) << 24) | ((slice[1] & 0xFF) << 16) | ((slice[2] & 0xFF) << 8) | (slice[3] & 0xFF);
	return num;
    }
    public static long byteToLong(byte[] slice)
    {
	long num = ((slice[0] & 0xFFL) << 56) | ((slice[1] & 0xFFL) << 48) | ((slice[2] & 0xFFL) << 40) | ((slice[3] & 0xFFL) << 32) | ((slice[4] & 0xFFL) << 24) | ((slice[5] & 0xFFL) << 16) | ((slice[6] & 0xFFL) << 8) | (slice[7] & 0xFFL);
	return num;
    }
}
