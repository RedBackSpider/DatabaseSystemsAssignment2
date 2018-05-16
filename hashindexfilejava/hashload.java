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
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class hashload
{
    public static void main(String[] args)
    {
	String ps = "";
	if(args.length != 1)
	    {
		System.out.println("Incorrect number of Arguments");
		System.exit(0);
	    }
	ps = args[0];
	int pagesize = 0;
        try
	    {
		pagesize = Integer.parseInt(ps);
		
	    }
        catch(NumberFormatException e)
	    {
		System.out.println("Non-numerical pagesize entered");
		System.exit(0);
	    }
	// Files to read and write to
	String filename = "heap." + ps;
	String outputname = "hash." + ps;
	long startTime = System.currentTimeMillis();
	long[] numbers = writeIndexesToFile(filename, outputname, pagesize);
	long endTime = System.currentTimeMillis();
	long duration = (endTime - startTime);
	System.out.println("Time taken in milliseconds: " + duration);
	System.out.println("Number of records searched: " + numbers[0]);
	System.out.println("Number of pages searched: " + numbers[1]);
    }

    public static long[] writeIndexesToFile(String filename, String outputname, int pagesize){
	FileInputStream fis = null;
	RandomAccessFile output = null;
	//FileOutputStream fos = null;
	//BufferedOutputStream fis = null;
	long numOfRecords = 0; 
	long numOfPages = 0; 
	try
	    {
		fis = new FileInputStream(filename);
		//BufferedReader sc = new BufferedReader(new FileReader(filename));
		// output is random access so that when a new hash index is inserted it can be inserted into the right spot
		output = new RandomAccessFile(new File(outputname), "rw");
		//fos = new FileOutputStream(output);
		// overall offset of the file from the start point;
		long totalOffSet = 0;
		// Size of Bucket in Bytes
		int bucketByteSize = 44004; // exactly divisible by 12
		//int bucketByteSize = 84;
		// Total number of buckets
		int numberOfBuckets = 1024;
		// Length of FileInputStream read
		int len = pagesize;
		// how far into the page the record is 
		int buffset = 0;
		// where the page from the heap file will be read into
		byte[] buffer = new byte[len]; 		
		// Size of the bucket
		int[] sizeOfHashBucket = new int[numberOfBuckets];
		while((len = fis.read(buffer)) != -1)
		    {
			// overall number of pages
			numOfPages++;
			// Get total number of records in page
			byte[] slice = Arrays.copyOfRange(buffer, 0, 8);
			long numberOfRecordsInPage = byteToLong(slice);
			// add buffset to overall Offset
			buffset = 8;
			totalOffSet += buffset;
			int recNum = 0;
			while(recNum < numberOfRecordsInPage)
			{
			    // overall number of records
			    numOfRecords++;
			    // Because this is a non-fixed length database, each record must be read in its entirety before creating the hashinex
			    // As all the string fields (BNNAME, BNSTATUS etc.) are variable (are not automatically set to the maximum length)
			    // Size of the heap file record itself
			    int recordSize = 0;		
			    // Read BUSINESS NAMES from file
			    byte[] slice1 = Arrays.copyOfRange(buffer, buffset, buffset + 14); 
			    String regname = new String(slice1);
			    buffset = buffset + 14;
			    recordSize = recordSize + 14;
			    // Read namelength to file
			    byte[] slice2 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int namelength = byteToInt(slice2);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnname from file
			    byte[] slice3 = Arrays.copyOfRange(buffer, buffset, buffset + namelength); 
			    String bnname = new String(slice3);
			    buffset = buffset + namelength;
			    recordSize = recordSize + namelength;
			    // read status length from file
			    byte[] slice4 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int statuslength = byteToInt(slice4);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnstatus from file
			    byte[] slice5 = Arrays.copyOfRange(buffer, buffset, buffset + statuslength); 
			    String bnstatus = new String(slice5);
			    buffset = buffset + statuslength;
			    recordSize = recordSize + statuslength;
			    // read regdate length from file
			    byte[] slice6 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int reglength = byteToInt(slice6);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read regdt from file
			    byte[] slice7 = Arrays.copyOfRange(buffer, buffset, buffset + reglength); 
			    String bnregdt = new String(slice7);
			    buffset = buffset + reglength;
			    recordSize = recordSize + reglength;
			    // read cancel date length from file
			    byte[] slice8 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int cancellength = byteToInt(slice8);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnstatus from file
			    byte[] slice9 = Arrays.copyOfRange(buffer, buffset, buffset + cancellength); 
			    String bncanceldt = new String(slice9);
			    buffset = buffset + cancellength;
			    recordSize = recordSize + cancellength;
			    // read renew length from file
			    byte[] slice10 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int renewlength = byteToInt(slice10);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnstatus from file
			    byte[] slice11 = Arrays.copyOfRange(buffer, buffset, buffset + renewlength); 
			    String bnrenewdt = new String(slice11);
			    buffset = buffset + renewlength;
			    recordSize = recordSize + renewlength;
			    // read status length from file
			    byte[] slice12 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int statenumlength = byteToInt(slice12);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnstatus from file
			    byte[] slice13 = Arrays.copyOfRange(buffer, buffset, buffset + statenumlength); 
			    String bnstatenum = new String(slice13);
			    buffset = buffset + statenumlength;
			    recordSize = recordSize + statenumlength;
			    // read status length from file
			    byte[] slice14 = Arrays.copyOfRange(buffer, buffset, buffset + 4); 
			    int statereglength = byteToInt(slice14);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    // read bnstatus from file
			    byte[] slice15 = Arrays.copyOfRange(buffer, buffset, buffset + statereglength); 
			    String bnstatereg = new String(slice15);
			    buffset = buffset + statereglength;
			    recordSize = recordSize + statereglength;
			    // read status length from file
			    byte[] slice16 = Arrays.copyOfRange(buffer, buffset, buffset + 8); 
			    long bnabn = byteToLong(slice16);
			    buffset = buffset + 8;
			    recordSize = recordSize + 8;
			    // set position of record in heap file to overall offset
			    byte[] fileos = longToByte(totalOffSet);
			    // increase offset by this record
			    totalOffSet += recordSize;
			    
			    // Get the value for hashquery to search for
			    int hashvalue = bnname.hashCode();
			    // Which Bucket to put the hash value, Maths.abs to make it positive
			    int hashindex = Math.abs(hashvalue % numberOfBuckets); 
			    
			    // convert the hashvalue to byte[]
			    byte[] value = intToByte(hashvalue);
			    // write hashvalue and heapfileofset to new bytearray
			    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
			    bytestream.write(value);
			    bytestream.write(fileos);
			    byte[] writeBytes = bytestream.toByteArray();
			    // size of hash index
			    int bytelength = writeBytes.length;
			    // index record is to be placed in if hash index is optimal
			    int startIndex = hashindex;
			    // continue to look for 
			    while(sizeOfHashBucket[hashindex] + bytelength > bucketByteSize)
				{
				    hashindex = (hashindex + 1) %  numberOfBuckets;
				    if (hashindex == startIndex)
					{
					    throw new IOException();
					}
				}
			    // how far from the start of the hash file to the start of the bucket
			    int bucketOffSet = hashindex * bucketByteSize;
			    // how far from the start of the hash file the start of the hash index in bucket
			    int hashfileOffSet = bucketOffSet + sizeOfHashBucket[hashindex];
			    // write the hashindex to the correct position
			    output.seek(hashfileOffSet);
			    output.write(writeBytes);
	
			    if(numOfRecords % 500000 == 0)
			    {
				System.out.println("Business Name: " + bnname + " Hash Value of Business Name: " + hashvalue+ " Bucket Index: " +  hashindex);
				System.out.println("Offset Heap: "  +(totalOffSet - recordSize) + " Record Size: " + recordSize + " Buffset: " + buffset);
				System.out.println("Offset Hash: "  + hashfileOffSet + " Bucket offset: " + bucketOffSet);
				System.out.println("writeBytes length: " + writeBytes.length);
				}
			    // Increase size of hash bucket by size of the hash index inserted into bucket
			    sizeOfHashBucket[hashindex] += bytelength;
			    // increase number of recs read
			    recNum++;
			}
			// increase offset by leftover bytes that have nothing in them
			int leftOver = pagesize - buffset;
			totalOffSet += leftOver;
		    }
	    }
	catch (FileNotFoundException e)
	    {
		System.out.println("File Not Found");
	    }
	catch (IOException e)
	    {
		System.out.println("Error in insertion, too many records");
	    }
	finally
	    {
		// Close all inputs and outputs
		if(output != null)
		    {
			try
			    {
				output.close();
			    }
			catch (IOException ex)
			    {

			    }
		    }
		if(fis != null)
		    {
			try
			    {
				fis.close();
			    }
			catch (IOException ex)
			    {
				
			    }
		    }
		// return statistics
		long[] numbers = {numOfRecords, numOfPages};
		return numbers;
	    }
    }
    // Bitwise operations to convert data type to byte array and back
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
