package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MailContent {

	//sql data structure
	private static Connection m_connection = null;
	private static PreparedStatement m_statement = null;
	private static ResultSet m_resultset = null;
	
	//sql command
	private static String sql_summary = "select adtype,sum(pv),sum(click) from ad_hour_pv_click where date=? group by adtype;";
	
	private static SimpleDateFormat m_date_format = new SimpleDateFormat("yyyy/MM/dd");
	
	private static Map<String,String> m_slgPV = new TreeMap<String,String>();
	private static Map<String,String> m_slgClick = new TreeMap<String,String>();
	
	private static Map<String,String> m_biPV = new TreeMap<String,String>();
	private static Map<String,String> m_biClick = new TreeMap<String,String>();
	
	private static Map<String,String> m_sqlPV = new TreeMap<String,String>();
	private static Map<String,String> m_sqlClick = new TreeMap<String,String>();
	
	public static void getSessionlogSum(String path)
	{
		try {
			BufferedReader bufReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			m_slgPV.clear();
			m_slgClick.clear();
			while(true)
			{
				String line = bufReader.readLine();
				if(line == null)
					break;
				
				line = line.trim();
				if(line.equals(""))
					continue;
				
				String[] tags = line.split("\t");
				if(tags.length != 3)
				{
					System.err.println("sessionlog summary format[adtype,pv,click] error:" + line);
					continue;
				}
				
				if(tags[0].equalsIgnoreCase("AdDisplay"))
				{
					if(tags[1].equalsIgnoreCase("NULL"))
						tags[1] = "null";
					m_slgPV.put(tags[1], tags[2]);
				}
				
				if(tags[0].equalsIgnoreCase("AdClick"))
				{
					if(tags[1].equalsIgnoreCase("NULL"))
						tags[1] = "null";
					m_slgClick.put(tags[1], tags[2]);
				}
			}
			bufReader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void getBIJobSum(String path)
	{
		try {
			BufferedReader bufReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			m_biPV.clear();
			m_biClick.clear();
			while(true)
			{
				String line = bufReader.readLine();
				if(line == null)
					break;
				
				line = line.trim();
				if(line.equals(""))
					continue;
				
				String[] tags = line.split("\t");
				if(tags.length != 3)
				{
					System.err.println("bi summary format[adtype,pv,click] error:" + line);
					continue;
				}
				
				m_biPV.put(tags[0], tags[1]);
				m_biClick.put(tags[0], tags[2]);
			}
			bufReader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void getMysqlSum(long dt)
	{
		try {
			DBUtil util = new DBUtil();
			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			m_statement = m_connection.prepareStatement(sql_summary);
			m_statement.setLong(1, dt);
			
			System.out.println("date:" + dt);
			m_resultset = m_statement.executeQuery();
			
			m_sqlPV.clear();
			m_sqlClick.clear();
			while(m_resultset.next())
			{
				m_sqlPV.put(m_resultset.getString(1), m_resultset.getString(2));
				m_sqlClick.put(m_resultset.getString(1), m_resultset.getString(3));
			}
			
			m_connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally{
			DBUtil.close(m_connection, m_statement, m_resultset);
		}
	}
	
	public static void buildMailContent(String path,String dt)
	{
		try {
			BufferedWriter bufWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
			bufWriter.write("Hello everyone:\n\n");
			bufWriter.write("Report Date:" + dt + "\n");
			bufWriter.write("======adtype pv click summary======\n");
			bufWriter.write("adtype\tsessionlog_pv:bijob_pv:mysql_pv\tsessionlog_click:bijob_click:mysql_click\n");
			//Set<String> keySet = m_sqlPV.keySet();
			Set<String> keySet = m_biPV.keySet();
			Iterator<String> keyIter = keySet.iterator();
			while(keyIter.hasNext())
			{
				String key = keyIter.next();
				bufWriter.write(key);
				bufWriter.write("\t" + m_slgPV.get(key));
				bufWriter.write(":" + m_biPV.get(key));
				bufWriter.write(":" + m_sqlPV.get(key));
				bufWriter.write("\t" + m_slgClick.get(key));
				bufWriter.write(":" + m_biClick.get(key));
				bufWriter.write(":" + m_sqlClick.get(key));
				bufWriter.newLine();
			}
			bufWriter.write("======daily report link======\n");
			bufWriter.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String sessionlog_sum_path = "D:\\Work\\WorkSVN\\BI\\AdBI\\sessionlog.sum";
		String bi_sum_path = "D:\\Work\\WorkSVN\\BI\\AdBI\\adbi.sum";
		String sdate = "2013/05/11";
		String content_path = "D:\\Work\\WorkSVN\\BI\\AdBI\\mail.cnt";
		
		if(args.length == 4)
		{
			sessionlog_sum_path = args[0];
			bi_sum_path = args[1];
			sdate = args[2];
			content_path = args[3];
		}
		
		long dt = 0l;
		try {
			dt = m_date_format.parse(sdate).getTime();
			System.out.println("begin to load sessionlog summary");
			getSessionlogSum(sessionlog_sum_path);
			System.out.println("begin to load bijob summary");
			getBIJobSum(bi_sum_path);
			System.out.println("begin to load mysql summary");
			getMysqlSum(dt);
			System.out.println("begin to build mail content");
			buildMailContent(content_path,sdate);
			System.out.println("finish");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
