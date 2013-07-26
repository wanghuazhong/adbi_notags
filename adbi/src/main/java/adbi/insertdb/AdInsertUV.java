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

public class AdInsertUV {

	private Connection conn;
	private PreparedStatement stm;
	private StringBuffer sql_ad_day_uv;

	private String dbTime;
	private String directPath;

	private MonitorKeyParser keyParser;
	private boolean sucess;
	private long count;

	public AdInsertUV(String time, String path) {
		conn = null;
		stm = null;
		sql_ad_day_uv = new StringBuffer(
				"insert into ad_day_uv"
						+ "(ad_key, camp_source, advertiser, campaign, adgroup, line, material, date, uv, adtype) "
						+ "VALUES(?, ?, ?, ?,?, ?, ?, ?,?, ?)");
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
			stm = conn.prepareStatement(sql_ad_day_uv.toString());
		} catch (ClassNotFoundException e) {
			sucess = false;
			e.printStackTrace();
		} catch (SQLException e) {
			sucess = false;
			e.printStackTrace();
		}
		System.out.println("AdDBInsertUV init!");
	}

	public void insert() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		File files = new File(directPath);
		BufferedReader bufReader = null;
		String[] tag = null;
		Timestamp tsp = null;
		Date date = null;

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
						if (splits.length != 2) {
							System.out
									.println("Input line format error, line is not two fileds. line:"
											+ record.toString());
							return;
						}

						tag = splits[0].split("\\^\\^");
						if (tag.length != 5) {
							System.out
									.println("Input line format error, key is not 5 fileds(statindex,statdim,adkey,adKeyStatus,adtype). key:"
											+ splits[0]);
							return;
						}

						String ad_key = tag[2];
						String adKeyStatus = tag[3];
						String adtype = tag[4];
						// LOG.info("ADTYPE:" + adtype);

						String uv = splits[1];
						try {
							date = sdf.parse(dbTime.trim());
							tsp = new Timestamp(date.getTime());
						} catch (ParseException e) {
							System.out.println("ParseException:"
									+ e.getMessage());
							e.printStackTrace();
						}

//						if (adKeyStatus.equals("0")) {
							String src = null;
							int adv, camp, adg, line, mat;
							adv = camp = adg = line = mat = 0;
							keyParser.set_monitor_key(ad_key);
							if (keyParser.parseMonitorKey()) {
								src = String.valueOf(keyParser.get_source_id());
								adv = (int) (keyParser.get_advertiser_id());
								camp = (int) keyParser.get_campaign_id();
								adg = (int) keyParser.get_adgroup_id();
								line = (int) keyParser.get_line_id();
								mat = (int) keyParser.get_material_id();
							}
							if (ad_key.length() < 101) {
								stm.setString(1, ad_key);
								stm.setString(2, src);
								stm.setInt(3, adv);
								stm.setInt(4, camp);
								stm.setInt(5, adg);
								stm.setInt(6, line);
								stm.setInt(7, mat);
								stm.setLong(8, tsp.getTime());
								stm.setLong(9, Long.parseLong(uv));
								stm.setString(10, adtype);
								stm.addBatch();

								count++;
								if (count % 10000 == 0) {
									stm.executeBatch();
								}
							}
//						} else {
//							System.out.println("illegal moniterkey:" + ad_key);
//						}

					}

				}
			} catch (FileNotFoundException e) {
				System.out.println("File not exsite:" + e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IOException:" + e.getMessage());
				e.printStackTrace();
			} catch (SQLException e) {
				System.out.println("SQLException map Error:" + e.getMessage());
				e.printStackTrace();
			} catch (NullPointerException e) {
				System.out.println("NullPointerException map Error:"
						+ e.getMessage());
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
		
		try{
			stm.executeBatch();
		}catch(SQLException e){
			e.printStackTrace();
		} 
	}

	public void cleanup() {
//		try {
//			if (sucess == true) {
//				stm.executeBatch();
//				conn.commit();
//			} else {
//				conn.rollback();
//				throw new NullPointerException(
//						"------>>>>>>>>>>conn.rollback()!");
//			}
//			System.out.println("sucess:" + sucess + ",total:" + count);
//		} catch (SQLException e) {
//			System.out.println("SQLException Error in cleanup:"
//					+ e.getMessage());
//			e.printStackTrace();
//		} catch (NullPointerException e) {
//			System.out.println("NullPointerException Error in cleanup:"
//					+ e.getMessage());
//			e.printStackTrace();
//		} finally {
			//				conn.setAutoCommit(true);
		    System.out.println("success insert:" + count + "uv records");
			DBUtil.close(conn, stm, null);
			System.out.println("AdDBInsertUV cleanup!");
		}
//	}
	
}
