package adbi.insertdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import util.DBUtil;
import util.MonitorKeyParser;

public class AdInsertInterest {


	private Connection conn;
	private PreparedStatement stm;
	private StringBuffer sql_ad_interest_pv_click;

	private String dbTime;
	private String directPath;

	private MonitorKeyParser keyParser;
	private boolean sucess;
	private long count;
	

	public AdInsertInterest(String time, String path) {		
		conn = null;
		stm = null;
		sql_ad_interest_pv_click = new StringBuffer("insert into ad_interest_pv_click"
				+ "(ad_key, camp_source, advertiser, campaign, adgroup, line, material,interest, pv, click, date, adtype,position,rotation) "
				+ "VALUES(?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?)");
		dbTime = time;
		directPath = path;
		keyParser = new MonitorKeyParser();
		sucess = true;
		count = 0l;
	}

	public void init() {
		try {
			DBUtil util = new DBUtil();
			conn = util.getConnection();
//			conn.setAutoCommit(false);
			stm = conn.prepareStatement(sql_ad_interest_pv_click.toString());
		} catch (ClassNotFoundException e) {
			sucess = false;
			e.printStackTrace();
		} catch (SQLException e) {
			sucess = false;
			e.printStackTrace();
		}
		System.out.println("AdDBInsertInterest init");
	}

	public void insert() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		File files = new File(directPath);
		BufferedReader bufReader = null;
		String[] tag = null;
		Date date = null;
		Timestamp tsp = null;
		
		if (files.isDirectory()) {
			String[] fileList = files.list();
			try {
				for (int i = 0; i < fileList.length; i++) {
					String record = null;
					bufReader = new BufferedReader(
							new InputStreamReader(new FileInputStream(
									directPath + '/' + fileList[i])));
					while ((record = bufReader.readLine()) != null) {
						String[] splits = record.toString().split("\\t");
						if(splits.length != 2)
						{
							System.out.println("Input line format error, line is not two fileds. line:" + record.toString());
							return;
						}
						
						tag = splits[0].split("\\^\\^");
						if(tag.length != 6)
						{
							System.out.println("Input line format error, key is not 6 fileds(statindex,statdim,adkey,interest,adpos,adtype). key:" + splits[0]);
							return;
						}
						
						String ad_key = tag[2];
						String interest = tag[3];
						String adpos = tag[4];
						String adtype = tag[5];

						tag = splits[1].split("\\^\\^");
						if(tag.length != 2)
						{
							System.out.println("Input line format error, value is not two fields(pv,click). value:" + splits[1]);
							return;
						}
						
						String ad_pv = tag[0];
						String ad_click = tag[1];
						
						
						try
						{
							date = sdf.parse(dbTime.trim());
							tsp = new Timestamp(date.getTime());
						}catch (ParseException e) {
								e.printStackTrace();
						}
						
						String pos = null;
						String rot = null;
						String[] pos_seg = adpos.split("-");
						if(pos_seg.length == 2)
						{
							String[] tmp_tag = pos_seg[0].split("_");
							if(tmp_tag.length == 2)
								pos = tmp_tag[1];
							else
								pos = tmp_tag[0];
							
							rot = pos_seg[1];
						}
						else
						{
							pos = "unknown-" + adpos;
							rot = "unknown-" + adpos;
						}
						
						String src = null;
						int adv,camp,adg,line,mat;
						adv = camp = adg = line = mat = 0;
						keyParser.set_monitor_key(ad_key);
						if(keyParser.parseMonitorKey())
						{
							src = String.valueOf(keyParser.get_source_id());
							adv = (int)(keyParser.get_advertiser_id());
							camp = (int) keyParser.get_campaign_id();
							adg = (int) keyParser.get_adgroup_id();
							line = (int) keyParser.get_line_id();
							mat = (int) keyParser.get_material_id();
						}
						if(ad_key.length() < 101 && pos.length() < 101 && rot.length() < 101){
							stm.setString(1, ad_key);
							stm.setString(2, src);
							stm.setInt(3, adv);
							stm.setInt(4, camp);
							stm.setInt(5, adg);
							stm.setInt(6, line);
							stm.setInt(7, mat);
							stm.setString(8, interest);
							stm.setLong(9, Long.parseLong(ad_pv));
							stm.setLong(10, Long.parseLong(ad_click));
							stm.setLong(11, tsp.getTime());
							stm.setString(12, adtype);
							stm.setString(13, pos);
							stm.setString(14, rot);
							stm.addBatch();

							count++;
							if (count % 100000 == 0) {
								stm.executeBatch();
								System.out.println("insert interest inside");
							}
						}
						
					}

				}
			} catch (FileNotFoundException e) {
				System.out.println("File not exsite:" + e.getMessage());
				 sucess = false;
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IOException:" + e.getMessage());
				 sucess = false;
				e.printStackTrace();
			} 
			catch (SQLException e) {
				System.out.println("SQLException map Error:" + e.getMessage());
				sucess = false;
				e.printStackTrace();
			} 
			catch (NullPointerException e) {
				System.out.println("NullPointerException map Error:"
						+ e.getMessage());
				sucess = false;
				e.printStackTrace();
			} finally {
				try {
					if (bufReader != null) {
						bufReader.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void cleanup() {
		try {
			if (sucess == true) {
				stm.executeBatch();
//				conn.commit();
				System.out.println("success insert");
			} else {
//				conn.rollback();
				throw new NullPointerException("------>>>>>>>>>>conn.rollback()!");
			}
			System.out.println("sucess:" + sucess + ",total:" + count);
		} catch (SQLException e) {
			System.out.println("SQLException Error in cleanup:" + e.getMessage());
			e.printStackTrace();
		} catch(NullPointerException e) {
			System.out.println("NullPointerException Error in cleanup:" + e.getMessage());
			e.printStackTrace();
		}
		finally {
			try {
				conn.setAutoCommit(true);
				DBUtil.close(conn, stm, null);
			} catch (SQLException e) {
				System.out.println("SQLException Error setAutoCommit cleanup:"
						+ e.getMessage());
				e.printStackTrace();
			}
			System.out.println("AdDBInsertInterest cleanup!");
		}
	}


}
