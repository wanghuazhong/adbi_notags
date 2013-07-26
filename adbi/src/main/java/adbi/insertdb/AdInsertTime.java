package adbi.insertdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import util.BidInfo;
import util.DBUtil;
import util.MonitorKeyParser;

public class AdInsertTime {

	private Connection conn;
	private PreparedStatement stm;
	private PreparedStatement stm_illegal_key;
	private StringBuffer sql_ad_hour_pv_click;
	private StringBuilder sql_illegal_monitor_key;

	private String dbTime;
	private String directPath;

	private MonitorKeyParser keyParser;
	private long count;
	private long illegal_count;
	private BidInfo m_bid_info;

	public AdInsertTime(String time, String path) {
		conn = null;
		stm = null;
		sql_ad_hour_pv_click = new StringBuffer(
				"insert into ad_hour_pv_click"
						+ "(ad_key, camp_source, advertiser, campaign, adgroup, line, material, time_date,time_hour, pv, click, date, adtype,position,rotation,revenue,new_revenue) "
						+ "VALUES(?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?,?,?,?)");
		sql_illegal_monitor_key = new StringBuilder(
				"insert into illegal_monitor_key"
						+ "(ad_key,time_date,time_hour,pv,click,date,adtype,position,rotation,revenue,status_code)"
						+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		dbTime = time;
		directPath = path;
		keyParser = new MonitorKeyParser();
		count = 0l;
		illegal_count = 0l;
		m_bid_info = new BidInfo();
	}

	public void init() {
		m_bid_info.initBidInfo();
//		m_bid_info.initBidInfoFromDayBid(dbTime);
		m_bid_info.updateLocalBidInfo(dbTime);
		try {
			DBUtil util = new DBUtil();
			conn = util.getConnection();
			stm = conn.prepareStatement(sql_ad_hour_pv_click.toString());
			stm_illegal_key = conn.prepareStatement(sql_illegal_monitor_key
					.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("AdDBInsertTime init!");
	}

	public void insert() {
		SimpleDateFormat ssdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		File files = new File(directPath);
		BufferedReader bufReader = null;
		String[] tag = null;
		Date date = null;

		if (files.isDirectory()) {
			String[] fileList = files.list();
			
			for (int i = 0; i < fileList.length; i++) {
				String record = null;
				try {
					bufReader = new BufferedReader(new InputStreamReader(
							new FileInputStream(directPath + '/' + fileList[i])));
					while ((record = bufReader.readLine()) != null) {
						String[] splits = record.toString().split("\\t");
						if (splits.length != 2) {
							System.out
									.println("Input line format error, line is not two fileds. line:"
											+ record.toString());
							continue;
						}

						tag = splits[0].split("\\^\\^");
						if (tag.length != 7) {
							System.out
									.println("Input line format error, key is not 6 fileds(statindex,statdim,adkey,adKeyStatus,time,adpos, adtype). key:"
											+ splits[0]);
							continue;
						}

						String ad_key = tag[2];
						String adKeyStatus = tag[3];
						String time_of_hour = tag[4];
						String adpos = tag[5];
						String adtype = tag[6];

						try {
							date = ssdf.parse(time_of_hour.trim());
						} catch (ParseException e) {
							e.printStackTrace();
						}

						tag = splits[1].split("\\^\\^");
						if (tag.length != 3) {
							System.out
									.println("Input line format error, value is not 3 fields. value:"
											+ splits[1]);
							continue;
						}

						String ad_pv = tag[0];
						String ad_click = tag[1];
						String new_revenue = tag[2];

						Date date1 = null;
						Timestamp tsp1 = null;
						try {
							date1 = sdf.parse(dbTime.trim());
							tsp1 = new Timestamp(date1.getTime());
						} catch (ParseException e) {
							e.printStackTrace();
						}

						String pos = null;
						String rot = null;
						String[] pos_seg = adpos.split("-");
						if (pos_seg.length == 2) {
							String[] tmp_tag = pos_seg[0].split("_");
							if (tmp_tag.length == 2)
								pos = tmp_tag[1];
							else
								pos = tmp_tag[0];

							rot = pos_seg[1];
						} else {
							pos = "unknown-" + adpos;
							rot = "unknown-" + adpos;
						}

						double revenue = 0.0;
						if (adtype.equalsIgnoreCase("1") || "0".equalsIgnoreCase(adtype))
							revenue = Double.valueOf(m_bid_info.getRevenue(
									ad_key, Long.valueOf(ad_pv),
									Long.valueOf(ad_click)));
						if (revenue < 0.0)
							revenue = 0.0;

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
							if (ad_key.length() < 101 && pos.length() < 101
									&& rot.length() < 101) {

								// System.out.println("revenue:" +
								// BigDecimal.valueOf(revenue));

								stm.setString(1, ad_key);
								stm.setString(2, src);
								stm.setInt(3, adv);
								stm.setInt(4, camp);
								stm.setInt(5, adg);
								stm.setInt(6, line);
								stm.setInt(7, mat);
								// stm.setLong(8,
								// sdf.parse(sdf.format(date)).getTime());
								stm.setLong(8, date.getTime());
								stm.setInt(9, date.getHours());
								stm.setLong(10, Long.parseLong(ad_pv));
								stm.setLong(11, Long.parseLong(ad_click));
								stm.setLong(12, tsp1.getTime());
								stm.setString(13, adtype);
								stm.setString(14, pos);
								stm.setString(15, rot);
								stm.setBigDecimal(16,
										BigDecimal.valueOf(revenue));
								stm.setBigDecimal(17, new BigDecimal(new_revenue));
//								stm.setDouble(17, Double.valueOf(new_revenue));
								stm.addBatch();

								count++;
								if (count % 10000 == 0) {
									stm.executeBatch();
								}
							}
							

//						}
						if (!adKeyStatus.equals("0")) {
							System.out.println("illegal_monitor_key:" + ad_key
									+ "  pv:" + ad_pv + "  click:" + ad_click);
							if (ad_key.length() < 501 && pos.length() < 101
									&& rot.length() < 101) {
								stm_illegal_key.setString(1, ad_key);
								stm_illegal_key.setLong(2, date.getTime());
								stm_illegal_key.setInt(3, date.getHours());
								stm_illegal_key.setLong(4,
										Long.parseLong(ad_pv));
								stm_illegal_key.setLong(5,
										Long.parseLong(ad_click));
								stm_illegal_key.setLong(6, tsp1.getTime());
								stm_illegal_key.setString(7, adtype);
								stm_illegal_key.setString(8, pos);
								stm_illegal_key.setString(9, rot);
								stm_illegal_key.setBigDecimal(10,
										BigDecimal.valueOf(revenue));
								stm_illegal_key.setString(11, adKeyStatus);

								stm_illegal_key.addBatch();
								
								illegal_count++;
								if(illegal_count % 1000 == 0){
									stm_illegal_key.executeBatch();
								}
							}
						}
					}
				} catch (FileNotFoundException e) {
					System.out.println("File not exsite:" + e.getMessage());
					e.printStackTrace();
				} catch (IOException e) {
					System.out.println("IOException:" + e.getMessage());
					e.printStackTrace();
				} catch (SQLException e) {
					System.out.println("SQLException map Error:"
							+ e.getMessage());
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
		}
		
		try{
			stm.executeBatch();
			System.out.println("test");
			stm_illegal_key.executeBatch();
		}catch(SQLException e){
			e.printStackTrace();
		} 
	}

	public void cleanup() {
//		try {
//			stm_illegal_key.executeBatch();
//			if (sucess == true) {
//				stm.executeBatch();
////				conn.commit();
//			} else {
////				conn.rollback();
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
	    System.out.println("success insert:" + count + "records");
	    System.out.println("success insert:" + illegal_count + "illegal monitor_key records");
//		System.out.println("sucess:" + sucess + ",total:" + count);
		try {
				DBUtil.close(conn, stm, null);
				if (stm_illegal_key != null) {
					stm_illegal_key.close();
				}
			} catch (SQLException e) {
				System.out.println("SQLException Error setAutoCommit cleanup:"
						+ e.getMessage());
				e.printStackTrace();
			}
			System.out.println("AdDBInsertTime cleanup!");
		}
	}

// }
