package adbi.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import util.ConstData;

public class testStatic {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SimpleDateFormat sdf_hour = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date();
		String timeOfHour = sdf_hour.format(new Date(date.getTime()));
		try {
			date = sdf_hour.parse(timeOfHour);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		date.setHours(date.getHours() + 10);
		System.out.println("date:" + timeOfHour.toString());
//		
//		System.out.println("ADBI_AdCommonStatCheckJob_Notags"
//			+ new SimpleDateFormat("yyyyMMdd").format(new Date().getTime())
//					.toString());
//		
//		LinkedHashMap<String,Integer> REDIS_SERVERS = new LinkedHashMap<String,Integer>();
//		{
//			REDIS_SERVERS.put("10.16.10.48", 6380);
//			REDIS_SERVERS.put("10.16.10.47", 6380);
//			REDIS_SERVERS.put("10.16.10.46", 6380);
//		}
//		for(Iterator<Entry<String, Integer>> iter = REDIS_SERVERS.entrySet().iterator();iter.hasNext();)
//		{
//			Map.Entry<String, Integer> ent = iter.next();
//			System.out.println("server:" + ent.getKey() + ":" + ent.getValue());
//		}
//		String a,b,c;
//		b = c = null;
//		a = b + c;
//		System.out.println("A:" + a);
//		System.out.println(ConstData.DM_DIR_MAP.get(ConstData.DM_INTEREST));
//		System.out.println(ConstData.DM_DIR_MAP.get(ConstData.DM_REGION));
//		System.out.println(ConstData.DM_DIR_MAP.get(ConstData.DM_TIME));
//		System.out.println(ConstData.DM_DIR_MAP.get(ConstData.DM_USER));
//		
//		BufferedReader joinReader;
//		try {
//			joinReader = new BufferedReader(new FileReader("user_tags_level1.txt"));
//			System.out.println(joinReader.readLine());
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		System.out.println(ConstData.getNetworkName("tbk-1234"));
	}

}
