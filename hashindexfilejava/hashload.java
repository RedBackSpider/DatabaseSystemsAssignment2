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
	int numberBuckets = 256; 
	try
	    {
		boolean isNextPage = true;
		boolean isNextRecord = true;
		FileInputStream fis = new FileInputStream(filename);
		BufferedReader sc = new BufferedReader(new FileReader(filename));
		FileOutputStream os = new FileOutputStream(outputname));
		long startTime = System.currentTimeMillis();
		long numOfRecords = 0;
		String line = sc.readLine();
		int pagenum = 0;
		while(isNextPage)
		    {
			byte[] page = new byte[pagesize];
			fis.read(page, 0, pagesize);
			isNextRecord = true;
			while(isNextRecord)
			{
			    
			}
			pagenum++;
		    }
	// Read each line of heap file
	// Keep count of page and number of records held by page
	// Get the BNNAME from the heap and convert to hashcode()
	// Bitmask it and find correct bucket
	// In Correct Bucket get number of records already written to bucket
	// Calculate offset from fixed size and write to file
	// Increase counter for bucket size
	
