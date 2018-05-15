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
	String filename = "heap." + ps;
	String outputname = "hash." + ps;
	try
	    {
		FileInputStream fis = new FileInputStream(filename);
		//BufferedReader sc = new BufferedReader(new FileReader(filename));
		// output is random access so that when a new hash index is inserted it can be inserted into the right spot
		RandomAccessFile output = new RandomAccessFile(new File(outputname), "rws");
		long startTime = System.currentTimeMillis();
		// Total Count of Records Inserted
		long TotalNumOfRecords = 0;
		// Total Count of Pages Viewed
		long TotalNummOfPages = 0;
		// Size of Bucket in Bytes
		int bucketByteSize = 44000;
		int numberOfBuckets = 1024;
		// Length of FileInputStream read
		int len = pagesize + 8;
		// how far into the page the record is 
		int buffset = 0;
		// Size of the record itself
		int recordSize = 0;
		// where the page from the heap file will be read into
		byte[] buffer = new byte[len]; 
		// overall offset of the file from the start point;
		long totalOffSet = 0;
		// Size of the bucket
		int[] sizeOfHashBucket = new int[numberOfBuckets];
		long numOfPages = 0;
		
		// While the end of all pages has not been found
		while((len = fis.read(buffer)) != -1)
		    {
			numOfPages++;
			// Get total number of records in page
			byte[] slice = Arrays.copyOfRange(buffer, 0, 8);
			long numberOfRecordsInPage = byteToLong(slice);
			// Set offset of page to long value
			buffset = 8;
			totalOffSet += buffset;
			int recNum = 0;
			while(recNum < numberOfRecordsInPage)
			{
			    // Because this is a non-fixed length database, each record must be read in its entirety before creating the hashinex
			    // As all the string fields (BNNAME, BNSTATUS etc.) are variable (are not automatically set to the maximum length)
			    recordSize = 0;
			    byte[] slice1 = Arrays.copyOfRange(buffer, buffset, buffset + 14); // Read BUSINESS NAMES from file
			    String regname = new String(slice1);
			    buffset = buffset + 14;
			    recordSize = recordSize + 14;

			    byte[] slice2 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // Read namelength to file
			    int namelength = byteToInt(slice2);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice3 = Arrays.copyOfRange(buffer, buffset, buffset + namelength); // read bnname from file
			    String bnname = new String(slice3);
			    buffset = buffset + namelength;
			    recordSize = recordSize + namelength;

			    byte[] slice4 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read status length from file
			    int statuslength = byteToInt(slice4);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice5 = Arrays.copyOfRange(buffer, buffset, buffset + statuslength); // read bnstatus from file
			    String bnstatus = new String(slice5);
			    buffset = buffset + statuslength;
			    recordSize = recordSize + statuslength;
			    
			    byte[] slice6 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read regdate length from file
			    int reglength = byteToInt(slice6);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;
			    
			    byte[] slice7 = Arrays.copyOfRange(buffer, buffset, buffset + reglength); // read regdt from file
			    String bnregdt = new String(slice7);
			    buffset = buffset + reglength;
			    recordSize = recordSize + reglength;
			    
			    byte[] slice8 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read cancel date length from file
			    int cancellength = byteToInt(slice8);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice9 = Arrays.copyOfRange(buffer, buffset, buffset + cancellength); // read bnstatus from file
			    String bncanceldt = new String(slice9);
			    buffset = buffset + cancellength;
			    recordSize = recordSize + cancellength;
			    
			    byte[] slice10 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read renew length from file
			    int renewlength = byteToInt(slice10);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice11 = Arrays.copyOfRange(buffer, buffset, buffset + renewlength); // read bnstatus from file
			    String bnrenewdt = new String(slice11);
			    buffset = buffset + renewlength;
			    recordSize = recordSize + renewlength;
			    
			    byte[] slice12 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read status length from file
			    int statenumlength = byteToInt(slice12);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice13 = Arrays.copyOfRange(buffer, buffset, buffset + statenumlength); // read bnstatus from file
			    String bnstatenum = new String(slice13);
			    buffset = buffset + statenumlength;
			    recordSize = recordSize + statenumlength;
			    
			    byte[] slice14 = Arrays.copyOfRange(buffer, buffset, buffset + 4); // read status length from file
			    int statereglength = byteToInt(slice14);
			    buffset = buffset + 4;
			    recordSize = recordSize + 4;

			    byte[] slice15 = Arrays.copyOfRange(buffer, buffset, buffset + statereglength); // read bnstatus from file
			    String bnstatereg = new String(slice15);
			    buffset = buffset + statereglength;
			    recordSize = recordSize + statereglength;
			    
			    byte[] slice16 = Arrays.copyOfRange(buffer, buffset, buffset + 8); // read status length from file
			    long bnabn = byteToLong(slice16);
			    buffset = buffset + 8;
			    recordSize = recordSize + 8;
			    // The index key within the bucket
			    int hashvalue = bnname.hashCode();
			    // Which Bucket to put the hash value
			    int hashindex = hashvalue % 255;
			    // The offset of the record from the start of the file
			    long pageOffSet = numOfPages * pagesize;
			    long heapfileOffSet = buffset + pageOffSet;
			    // increase the overall offset of the file (where the next file is located)
			    recordSize = 0;
			    byte[] value = intToByte(hashvalue);
			    byte[] fileos = longToByte(heapfileOffSet);
			    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
			    bytestream.write(value);
			    bytestream.write(fileos);
			    byte[] writeBytes = bytestream.toByteArray();
			    int bytelength = writeBytes.length;
			    while(sizeOfHashBucket[hashindex] + bytelength > bucketByteSize)
				{
				    hashindex = (hashindex + 1) % numberOfBuckets;
				}
			    long bucketOffSet = hashindex * bucketByteSize;
			    long hashfileOffSet = bucketOffSet + sizeOfHashBucket[hashindex];
			    output.seek(hashfileOffSet);
			    output.write(writeBytes);
			    sizeOfHashBucket[hashindex] += bytelength;
			    recNum++;
			}
		    }
	    }
	catch (FileNotFoundException e)
	    {
		System.out.println("File Not Found");
		System.exit(0);	    
	    }
	catch (IOException e)
	    {
		e.printStackTrace();
		System.exit(0);	    
	    }
    }
    // Read each line of heap file
    // Keep count of page and number of records held by page
    // Get the BNNAME from the heap and convert to hashcode()
    // Bitmask it and find correct bucket
    // In Correct Bucket get number of records already written to bucket
    // Calculate offset from fixed size and write to file
    // Increase counter for bucket size
    
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
