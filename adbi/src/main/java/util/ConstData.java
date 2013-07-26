package util;

import java.util.HashSet;
import java.util.Hashtable;

public class ConstData {
	//sessionlog datatype
	public static final String CG_USER = "user";
	public static final String CG_PV = "pv";
	public static final String CG_SEARCH = "search";
	public static final String CG_ADDISPLAY = "addisplay";
	public static final String CG_ADCLICK = "adclick";
	
	public static final String CG_FIELDS = CG_USER + "," + CG_ADCLICK + "," + CG_ADDISPLAY;
	
	//job param
	public static final int MAX_REDUCE = 300;
	
	//index key flags
	public static final String ID_VISIT = "P";
	public static final String ID_REACH = "R";
	
	//data type flag
	public static final String DT_USERTAG = "T";
	public static final String DT_ADDISPLAY = "V";
	public static final String DT_ADCLICK = "C";
	public static final String DT_USERVIST = "U";
	
	//计费方式
	public static final String PRICETYPE = "price1";
	
	//stat dimension flag
	public static final String DM_TIME = "T";
	public static final String DM_REGION = "R";
	public static final String DM_INTEREST = "I";
	public static final String DM_USER = "U";
	
	//idex dirname
	public static Hashtable<String,String> ID_DIR_MAP = new Hashtable<String,String>();
	static
	{
		ID_DIR_MAP.put(ID_VISIT, "Visit");
		ID_DIR_MAP.put(ID_REACH, "Reach");
	}
	
	//dim dirname
	public static Hashtable<String,String> DM_DIR_MAP = new Hashtable<String,String>();
	static
	{
		DM_DIR_MAP.put(DM_TIME, "PVClick/Time");
		DM_DIR_MAP.put(DM_REGION, "PVClick/Region");
		DM_DIR_MAP.put(DM_INTEREST, "PVClick/Interest");
		DM_DIR_MAP.put(DM_USER, "UV");
	}
	
	//excluding tag type
	public static HashSet<Long> EXC_TAG_SET = new HashSet<Long>();
	static
	{
		EXC_TAG_SET.add(4l<<48);//title key words
		EXC_TAG_SET.add(5l<<48);//content key words
		EXC_TAG_SET.add(6l<<48);//query
	}
	
	//monitor key configure
	public static final int MONITOR_KEY_LENGTH = 25;
	public static final String MK_SOURCE = "source_id";
	public static final String MK_ADVERTISER = "advertiser_id";
	public static final String MK_COMPAIGN = "campaign_id";
	public static final String MK_ADGROUP = "adgroup_id";
	public static final String MK_LINE = "line_id";
	public static final String MK_MATERIAL = "material_id";
	public static Hashtable<String,Integer> MONITOR_KEY_FIELD_LENGTH = new Hashtable<String,Integer>();
	static
	{
		MONITOR_KEY_FIELD_LENGTH.put(MK_SOURCE, 1);
		MONITOR_KEY_FIELD_LENGTH.put(MK_ADVERTISER, 4);
		MONITOR_KEY_FIELD_LENGTH.put(MK_COMPAIGN, 4);
		MONITOR_KEY_FIELD_LENGTH.put(MK_ADGROUP, 5);
		MONITOR_KEY_FIELD_LENGTH.put(MK_LINE, 5);
		MONITOR_KEY_FIELD_LENGTH.put(MK_MATERIAL, 6);
	}
	
	//sql server configure
	//adpvinsight database
//	public static final String ADPVINSIGHT_SERVER = "192.168.110.29";
	public static final String ADPVINSIGHT_SERVER = "10.16.17.67";
	public static final String ADPVINSIGHT_PORT = "3306";
	public static final String ADPVINSIGHT_DB = "sohu_ad_pvinsight";
//	public static final String ADPVINSIGHT_USER = "root";
//	public static final String ADPVINSIGHT_PASSWORD = "1234";
	public static final String ADPVINSIGHT_USER = "adpv";
	public static final String ADPVINSIGHT_PASSWORD = "sohuadpv!(0#!";
	
/*	//grape database
	public static String GRAPE_SERVER = "10.16.17.69";
	public static String GRAPE_PORT = "3306";
	public static String GRAPE_DB = "grape";
	public static String GRAPE_USER = "grapeuser";
	public static String GRAPE_PASSWORD = "qFKMpczX5";
	//public static String ADSRV_USER = "loguser";
	//public static String ADSRV_PASSWORD = "123$%^";
	*/
	//adsrv database
	public static String ADSRV_SERVER = "10.10.57.76";
	public static String ADSRV_PORT = "3306";
	public static String ADSRV_DB = "adserv";
	public static String ADSRV_USER = "adservuser";
	public static String ADSRV_PASSWORD = "qZyG3ijRC";
//	public static String ADSRV_USER = "loguser";
//	public static String ADSRV_PASSWORD = "123$%^";
	
	//campaign source
	public static Hashtable<String,String> CAMPAIGN_SOURCE = new Hashtable<String,String>();
	static
	{
		CAMPAIGN_SOURCE.put("1", "CPD");
		CAMPAIGN_SOURCE.put("2", "DIY");
	}
	
	//ad type
	public static Hashtable<String,Integer> AD_TYPE = new Hashtable<String,Integer>();
	static
	{
		AD_TYPE.put("NEWS_AD",1);
		AD_TYPE.put("NEWS",2);
		AD_TYPE.put("NETWORK_AD", 3);
		AD_TYPE.put("BLOG_AD", 4);
	}
	
	//network name
	public static String getNetworkName(String str)
	{
		if(str.matches("200ga001i0000100001000001"))
			return "閺呰桨绨导妞剧喘閸忓牏楠�80";
		else if(str.matches("200ga000k0000100001000001"))
			return "閺呰桨绨导妞剧喘閸忓牏楠�20";
		else if(str.matches("200gb000k0000100001000001"))
			return "閻ф儳瀹抽懕鏃傛礃";
		else if(str.matches("tbk-.*"))
			return str;
			//return "婵烇絾锚閻ゅ倻锟介敓锟�
		else
			return "unknown-" + str;
	}
}
