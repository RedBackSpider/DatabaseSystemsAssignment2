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
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class dbload
{
    public static void main(String[] args)
    {
	String filename = "";
	String ps = "";
	if(args.length != 3)
	{
	    System.out.println("Incorrect number of Arguments");
	    System.exit(0);
	}
	if(args[0].equals("-p"))
	{
	    ps = args[1];
	    filename = args[2];
	}
	else if(args[1].equals("-p"))
	{
	    ps = args[2];
	    filename = args[0];
	}
	else
	{
	    System.out.println("Wrong Argument Format: Aguments must have a -p flag");
	    System.exit(0);
	}
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
	String outputname = "heap." + ps;
	ArrayList<HFPage> UnfilledPages = new ArrayList<>();
	try
	{
	    BufferedReader sc = new BufferedReader(new FileReader(filename));
	    FileOutputStream os = new FileOutputStream(outputname);
	    long startTime = System.currentTimeMillis();
	    int numOfRecords = 0;
	    sc.readLine(); // Do not use first line
	    String line = sc.readLine();
	    int currentPageSize = 0;
	    int currentNumberOfRecords = 0;
	    int numberOfPages = 1;
	    ArrayList<HFRecord> page = new ArrayList<HFRecord>();
	    while(line != null)
	    {
		ArrayList<String> linetokens = new ArrayList<String>(Arrays.asList(line.split("\t", -1)));
		if(linetokens.size() != 9)
		{
		    System.out.println("Wrong number of tokens");
		    sc.close();
		    os.close();
		    System.exit(0);
		}
		for(int i = 0; i < linetokens.size(); i++)
		{
		    if(linetokens.get(i).length() < 2)
		    {
			continue;
		    }
		    if(linetokens.get(i).charAt(0) == '"')
		    {
			linetokens.set(i, linetokens.get(i).substring(1,linetokens.get(i).length()));
		    }
		    if(linetokens.get(i).charAt(linetokens.get(i).length()-1) == '"')
		    {
			linetokens.set(i, linetokens.get(i).substring(0,linetokens.get(i).length()-1));
		    }
		}
		long abn = -1;
		if(isLong(linetokens.get(8)))
		{
		    abn = Long.parseLong(linetokens.get(8));
		}		
                HFRecord test = new HFRecord(linetokens.get(0), linetokens.get(1), linetokens.get(2), linetokens.get(3), linetokens.get(4), linetokens.get(5), linetokens.get(6), linetokens.get(7), abn);
                if(test.getByteSize() > pagesize)
		{
		    System.out.println("Record cannot fit into page structure, increase page size");
		    sc.close();
		    os.close();
		    System.exit(0);
		}
		numOfRecords++;
		line = sc.readLine();
		if(currentPageSize + test.getByteSize() > pagesize - 8)
		{
		    Iterator<HFRecord> it = page.iterator();
		    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
		    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		    buffer.putLong(currentNumberOfRecords);
		    byte[] writeBytes;
		    bytestream.write(buffer.array());
		    while(it.hasNext())
			{
			    HFRecord record = it.next();
			    byte[] registername = record.getRegName().getBytes();
			    bytestream.write(registername);
			    
			    byte[] bnname = record.getBNName().getBytes();	
			    int bnnamesize = record.getBNNameSize();
			    byte[] namelength = intToByte(bnnamesize);
			    bytestream.write(namelength);
			    bytestream.write(bnname);
			    
			    byte[] bnstatus = record.getStatus().getBytes();
			    int bnstatussize = record.getStatusSize();
			    byte[] statuslength = intToByte(bnstatussize);
			    bytestream.write(statuslength);
			    bytestream.write(bnstatus);
			    
			    byte[] bnregdate = record.getRegDt().getBytes();
			    int bnregdatesize = record.getRegDtSize();
			    byte[] reglength = intToByte(bnregdatesize);
			    bytestream.write(reglength);
			    bytestream.write(bnregdate);
			    
			    byte[] bncanceldate = record.getCancelDt().getBytes();
			    int bncanceldatesize = record.getCancelDtSize();
			    byte[] cancellength = intToByte(bncanceldatesize);
			    bytestream.write(cancellength);
			    bytestream.write(bncanceldate);
			    
			    byte[] bnrenewdate = record.getRenewDt().getBytes();
			    int bnrenewdatesize = record.getRenewDtSize();
			    byte[] renewlength = intToByte(bnrenewdatesize);
			    bytestream.write(renewlength);
			    bytestream.write(bnrenewdate);
			    
			    byte[] bnstatenum = record.getStateNum().getBytes();
			    int bnstatenumsize = record.getStateNumSize();
			    byte[] statenumlength = intToByte(bnstatenumsize);
			    bytestream.write(statenumlength);
			    bytestream.write(bnstatenum);
			    
			    byte[] bnstatereg = record.getStateReg().getBytes();
			    int bnstateregsize = record.getStateRegSize();
			    byte[] statereglength = intToByte(bnstateregsize);
			    bytestream.write(statereglength);
			    bytestream.write(bnstatereg);
			    
			    long bnabn = record.getABN();
			    byte[] abnbyte = longToByte(bnabn);
			    bytestream.write(abnbyte);
			}
		    writeBytes = bytestream.toByteArray();
		    byte[] paddingBytes = new byte[pagesize-writeBytes.length];
		    os.write(writeBytes,0,writeBytes.length);
		    os.write(paddingBytes,0,paddingBytes.length);
		    page.clear();
		    page.add(test);
		    numberOfPages++;
		    currentPageSize = test.getByteSize();
		    currentNumberOfRecords = 1;
		}
		else
		{
		    page.add(test);
		    currentPageSize += test.getByteSize();
		    currentNumberOfRecords++;
		}
	    }

	    Iterator<HFRecord> it = page.iterator();
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(currentNumberOfRecords);
	    byte[] writeBytes;
	    bytestream.write(buffer.array());
	    while(it.hasNext())
		{
		    HFRecord record = it.next();
		    byte[] registername = record.getRegName().getBytes();
		    bytestream.write(registername);
			    
		    byte[] bnname = record.getBNName().getBytes();	
		    int bnnamesize = record.getBNNameSize();
		    byte[] namelength = intToByte(bnnamesize);
		    bytestream.write(namelength);
		    bytestream.write(bnname);
			    
		    byte[] bnstatus = record.getStatus().getBytes();
		    int bnstatussize = record.getStatusSize();
		    byte[] statuslength = intToByte(bnstatussize);
		    bytestream.write(statuslength);
		    bytestream.write(bnstatus);
			    
		    byte[] bnregdate = record.getRegDt().getBytes();
		    int bnregdatesize = record.getRegDtSize();
		    byte[] reglength = intToByte(bnregdatesize);
		    bytestream.write(reglength);
		    bytestream.write(bnregdate);
			    
		    byte[] bncanceldate = record.getCancelDt().getBytes();
		    int bncanceldatesize = record.getCancelDtSize();
		    byte[] cancellength = intToByte(bncanceldatesize);
		    bytestream.write(cancellength);
		    bytestream.write(bncanceldate);
			    
		    byte[] bnrenewdate = record.getRenewDt().getBytes();
		    int bnrenewdatesize = record.getRenewDtSize();
		    byte[] renewlength = intToByte(bnrenewdatesize);
		    bytestream.write(renewlength);
		    bytestream.write(bnrenewdate);
			    
		    byte[] bnstatenum = record.getStateNum().getBytes();
		    int bnstatenumsize = record.getStateNumSize();
		    byte[] statenumlength = intToByte(bnstatenumsize);
		    bytestream.write(statenumlength);
		    bytestream.write(bnstatenum);
			    
		    byte[] bnstatereg = record.getStateReg().getBytes();
		    int bnstateregsize = record.getStateRegSize();
		    byte[] statereglength = intToByte(bnstateregsize);
		    bytestream.write(statereglength);
		    bytestream.write(bnstatereg);
			    
		    long bnabn = record.getABN();
		    byte[] abnbyte = longToByte(bnabn);
		    bytestream.write(abnbyte);
		}
	    writeBytes = bytestream.toByteArray();
	    byte[] paddingBytes = new byte[pagesize+8-writeBytes.length];
	    os.write(writeBytes,0,writeBytes.length);
	    os.write(paddingBytes,0,paddingBytes.length);    
	
	    long endTime = System.currentTimeMillis();
	    long duration = (endTime - startTime);
	    System.out.println("Time taken in milliseconds: " + duration);
	    System.out.println("Number of records inserted: " + numOfRecords);
	    System.out.println("Number of pages made: " + numberOfPages + 1);
	    sc.close();
	    os.close();
	}
    catch(FileNotFoundException e)
	{
	    e.printStackTrace();
	}
    catch(IOException e)
	{
	    e.printStackTrace();
	}
    }
    public static boolean isLong(String number)
    {
	try
	{
	    long l = Long.parseLong(number);
	}
	catch(NumberFormatException e)
	{
	    return false;
	}
	return true;
    }
    public static byte[] intToByte(int length)
    {
    	byte[] blength = new byte[4];
	blength[0] = (byte) (length >> 24);
	blength[1] = (byte) (length >> 16);
	blength[2] = (byte) (length >> 8);
	blength[3] = (byte) (length);
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
}
