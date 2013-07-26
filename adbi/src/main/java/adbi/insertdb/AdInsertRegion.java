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

public class AdInsertRegion {

	private Connection conn;
	private PreparedStatement stm;
	private StringBuffer sql_ad_city_pv_click;

	private String dbTime;
	private String directPath;

	private MonitorKeyParser keyParser;
	private boolean sucess;
	private long count;
	

	public AdInsertRegion(String time, String path) {		
		conn = null;
		stm = null;
		sql_ad_city_pv_click = new StringBuffer(
				"insert into new_ad_city_pv_click"
						+ "(ad_key, camp_source, advertiser, campaign, adgroup, line, material, country, province, city, pv, click, date, adtype) "
						+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)");
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
			stm = conn.prepareStatement(sql_ad_city_pv_click.toString());
		} catch (ClassNotFoundException e) {
			sucess = false;
			e.printStackTrace();
		} catch (SQLException e) {
			sucess = false;
			e.printStackTrace();
		}
		System.out.println("AdDBInsertRegion init!");
	}

	public void insert() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		File files = new File(directPath);
		BufferedReader bufReader = null;
		String[] tag = null;

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
						if (tag.length != 6) {
							System.out
									.println("Input line format error, key is not 6 fileds(statindex,statdim,adkey,adKeyStatus,region,adtype). key:"
											+ splits[0]);
							return;
						}

						String ad_key = tag[2];
						String adKeyStatus = tag[3];
						String region = tag[4];
//						String adpos = tag[4];
						String adtype = tag[5];

						try {
							long lregion = Long.valueOf(region);
							long mask = 0x0000ffffffff0000l;
							lregion = (lregion & mask) >> 16;
							region = String.valueOf(lregion);
						} catch (NumberFormatException e) {
							e.printStackTrace();
						}

						String country, province, city;
						if (region.length() != 10) {
							System.out
									.println("region format error, length not 10:"
											+ region);
							country = province = city = "unknown-" + region;
						} else {
							country = region.substring(0, 4);
							province = region.substring(4, 6);
							city = region.substring(6, 8);
						}

						tag = splits[1].split("\\^\\^");
						if (tag.length != 3) {
							System.out
									.println("Input line format error, value is not two fields. value:"
											+ splits[1]);
							return;
						}

						String ad_pv = tag[0];
						String ad_click = tag[1];

						Date date = null;
						Timestamp tsp = null;
						try {
							date = sdf.parse(dbTime.trim());
							tsp = new Timestamp(date.getTime());
						} catch (ParseException e) {
							e.printStackTrace();
						}

//						String pos = null;
//						String rot = null;
//						String[] pos_seg = adpos.split("-");
//						if (pos_seg.length == 2) {
//							String[] tmp_tag = pos_seg[0].split("_");
//							if (tmp_tag.length == 2)
//								pos = tmp_tag[1];
//							else
//								pos = tmp_tag[0];
//							rot = pos_seg[1];
//						} else {
//							pos = "unknown-" + adpos;
//							rot = "unknown-" + adpos;
//						}
//						if(adKeyStatus.equals("0")){
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
//							System.out.println(ad_key + " "
//									+ src + " "
//									+ adv + " "
//									+ camp + " "
//									+ adg + " "
//									+ line + " "
//									+ mat + " "
//									+ country + " "
//									+ province + " "
//									+ city + " "
//									+ ad_pv + " "
//									+ ad_click + " "
//									+ tsp.getTime() + " "
//									+ adtype + " "
//									+ pos + " "
//									+ rot);
							if(ad_key.length() < 101 && region.length() < 90) {
								stm.setString(1, ad_key);
								stm.setString(2, src);
								stm.setInt(3, adv);
								stm.setInt(4, camp);
								stm.setInt(5, adg);
								stm.setInt(6, line);
								stm.setInt(7, mat);
								stm.setString(8, country);
								stm.setString(9, province);
								stm.setString(10, city);
								stm.setLong(11, Long.parseLong(ad_pv));
								stm.setLong(12, Long.parseLong(ad_click));
								stm.setLong(13, tsp.getTime());
								stm.setString(14, adtype);
								stm.addBatch();

								count++;
								if (count % 10000 == 0) {
									stm.executeBatch();
								}
							}
//						}
//						else{
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
			} 
			catch (SQLException e) {
				System.out.println("SQLException map Error:" + e.getMessage());
				e.printStackTrace();
			} 
			catch (NullPointerException e) {
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
//				throw new NullPointerException("------>>>>>>>>>>conn.rollback()!");
//			}
//			System.out.println("sucess:" + sucess + ",total:" + count);
//		} catch (SQLException e) {
//			System.out.println("SQLException Error in cleanup:" + e.getMessage());
//			e.printStackTrace();
//		} catch(NullPointerException e) {
//			System.out.println("NullPointerException Error in cleanup:" + e.getMessage());
//			e.printStackTrace();
//		}
//		finally {
		System.out.println("success insert:" + count + "region records");
			//				conn.setAutoCommit(true);
			DBUtil.close(conn, stm, null);
			System.out.println("AdDBInsertRegion cleanup!");
//		}
	}

}
