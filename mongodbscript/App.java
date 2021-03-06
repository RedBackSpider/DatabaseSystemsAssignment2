import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.lang.StringBuilder;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.io.FileWriter;
import org.json.simple.*;

public class App
{
    public static void main( String[] args )
    {
        try
	    {
		String csvFile1 = "/home/ec2-user/BUSINESS_NAMES_201803.csv";
		//String csvFile1 = "/home/ec2-user/test.csv";
		Scanner sc = new Scanner(new File(csvFile1));
		//String csvFile2 = "/home/ec2-user/BUSINESS_NAMES_201803-1.csv";
		String jsonFile = "/home/ec2-user/BUSINESS_NAMES_201803.json";
		//String jsonFile = "/home/ec2-user/test.json";
		FileWriter writer = new FileWriter(jsonFile);
		sc.nextLine();
		int i = 0;
		while(sc.hasNextLine())
		    {
			i++;
			String line = sc.nextLine();
			ArrayList<String> linetokens = new ArrayList<String>(Arrays.asList(line.split("\t", -1)));
			JSONObject map = new JSONObject();
			//map.put()
			//Iterator<String> itr = linetokens.iterator();
			long abn = 0;
			if(linetokens.get(8).length() != 0)
			{
			    abn = Long.parseLong(linetokens.get(8));
			}
			map.put("REGISTER_NAME", linetokens.get(0));
			map.put("BN_NAME", linetokens.get(1));
			map.put("BN_STATUS", linetokens.get(2));
			JSONArray jsonarray = new JSONArray();
			JSONObject dates = new JSONObject();
			dates.put("BN_REG_DT", linetokens.get(3));
			dates.put("BN_CANCEL_DT", linetokens.get(4));
			dates.put("BN_RENEW_DT", linetokens.get(5));
			map.put("BN_DATES", dates);
			JSONObject states = new JSONObject();
			states.put("BN_STATE_NUM", linetokens.get(6));
			states.put("BN_STATE_REG", linetokens.get(7));
			map.put("BN_STATES", states);
			map.put("ABN", abn);
			//writer.append(writeline.toString());
			//System.out.println(map.toString());
			map.writeJSONString(writer);
			writer.append("\n");
		    }
		writer.flush();
		writer.close();
	    }
        catch (Exception e)
	    {
		e.printStackTrace();
	    }
    }
}
