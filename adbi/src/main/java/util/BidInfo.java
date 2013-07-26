package util;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BidInfo {
	//sql data structure
	private static Connection m_connection = null;
	private Statement m_statement = null;
	private PreparedStatement m_update_statement = null;
	private ResultSet m_resultset = null;
	
	//local data structure dumping from sql
	private Map<String,BigDecimal> m_bid_map = new HashMap<String,BigDecimal>();
	private Map<String,Byte> m_bidmode_map = new HashMap<String,Byte>();
	
	//sql command
	private String sql_bid = "select monitor_key,bid,bidmode from addelivery";
	private String sql_bid_update = "insert into ad_day_bid(ad_key,bid,bidmode,date) values(?,?,?,?)";
	
	
	public double getRevenue(String key,long pv,long click)
	{
		Byte bidmode = m_bidmode_map.get(key);
		BigDecimal bid = m_bid_map.get(key);
		
		if(bidmode == null)
			return -1.0;
		
		if(bid == null)
			return -2.0;
		
		double res = 0.0;
		switch(bidmode)
		{
		case 0:
			return -3.0;
		case 1://cpm
			res = pv * bid.doubleValue() / 1000.0;
			break;
		case 2://cpc
			res = click * bid.doubleValue();
			break;
		case 3://cps
			//res = pv * bid.doubleValue() / 1000.0;
			res = -1.0;//not support
			break;
		case 4://cpa
			//res = pv * bid.doubleValue() / 1000.0;
			res = -1.0;//not support
			break;
		case 5://roi
			//res = pv * bid.doubleValue() / 1000.0;
			res = -1.0;//not support
			break;
		default:
			System.out.println("no support for bidmode:" + bidmode);
			return -4.0;
		}
		return res;
	}
	
	public void initBidInfo()
	{
		try {
			DBUtil util = new DBUtil();
			m_connection = util.getConnection(ConstData.ADSRV_SERVER, ConstData.ADSRV_PORT, ConstData.ADSRV_DB, 
					ConstData.ADSRV_USER, ConstData.ADSRV_PASSWORD);
			m_statement = m_connection.createStatement();
		
			
			m_bid_map.clear();
			m_bidmode_map.clear();
			
			m_resultset = m_statement.executeQuery(sql_bid);
			while(m_resultset.next())
			{
				//System.out.println("key:" + m_resultset.getString(1) + "value:" + m_resultset.getBigDecimal(2));
				m_bid_map.put(m_resultset.getString(1), m_resultset.getBigDecimal(2));
				m_bidmode_map.put(m_resultset.getString(1), m_resultset.getByte(3));
			}
			
			m_connection.close();
			
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFoundException setup Error:" + e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println("SQLException setup Error:" + e.getMessage());
			e.printStackTrace();
		} finally{
			DBUtil.close(m_connection, m_statement, m_resultset);
		}
	}
	
	public void initBidInfoFromDayBid(String time)
	{
		try {
			DBUtil util = new DBUtil();

			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			
//			m_connection = util.getConnection("10.16.17.67", ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
//					"adpv", "sohuadpv!(0#!");
			
			m_statement = m_connection.createStatement();
		
			
			m_bid_map.clear();
			m_bidmode_map.clear();
			
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			Date date = null;
			Timestamp tsp = null;
			try {
				date = sdf.parse(time.trim());
				tsp = new Timestamp(date.getTime());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long dt = tsp.getTime();
		    
			String sql_day_bid = "select ad_key,bid,bidmode from ad_day_bid where date =" + dt;
			
			System.out.println(sql_day_bid);
			
			m_resultset = m_statement.executeQuery(sql_day_bid);
			while(m_resultset.next())
			{
				//System.out.println("key:" + m_resultset.getString(1) + "value:" + m_resultset.getBigDecimal(2));
				m_bid_map.put(m_resultset.getString(1), m_resultset.getBigDecimal(2));
				m_bidmode_map.put(m_resultset.getString(1), m_resultset.getByte(3));
			}
			
			m_connection.close();
			
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFoundException setup Error:" + e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println("SQLException setup Error:" + e.getMessage());
			e.printStackTrace();
		} finally{
			DBUtil.close(m_connection, m_statement, m_resultset);
		}
	}
	
	public BigDecimal getBid(String key)
	{
		if(m_bid_map.containsKey(key))
			return m_bid_map.get(key);
		else
			return new BigDecimal(0.0);
	}
	
	public Byte getBidMode(String key)
	{
		if(m_bidmode_map.containsKey(key))
			return m_bidmode_map.get(key);
		else
			return -1;
	}
	
//	public void updateLocalBidInfo(long dt)
//	{
//		try {
//			DBUtil util = new DBUtil();
//			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
//					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
//			m_update_statement = m_connection.prepareStatement(sql_bid_update);
//			
//			m_connection.setAutoCommit(false);
//			Set<String> sKey = m_bid_map.keySet();
//			Iterator<String> sIter = sKey.iterator();
//			while(sIter.hasNext())
//			{
//				String skey = sIter.next();
//				BigDecimal bid = m_bid_map.get(skey);
//				Byte bidmode = m_bidmode_map.get(skey);
//				
//				m_update_statement.setString(1, skey);
//				m_update_statement.setBigDecimal(2, bid);
//				m_update_statement.setByte(3, bidmode);
//				m_update_statement.setLong(4, dt);
//				//m_update_statement.execute();
//				m_update_statement.addBatch();
//			}
//			
//			m_update_statement.executeBatch();
//			m_connection.commit();
//			m_connection.close();
//			
//		} catch (ClassNotFoundException e) {
//			System.err.println("ClassNotFoundException setup Error:" + e.getMessage());
//			e.printStackTrace();
//		} catch (SQLException e) {
//			System.err.println("SQLException setup Error:" + e.getMessage());
//			e.printStackTrace();
//		} finally {
//			DBUtil.close(m_connection, m_update_statement, null);
//		}
//	}
	
	public void updateLocalBidInfo(String time)
	{
		try {
			DBUtil util = new DBUtil();
			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			m_update_statement = m_connection.prepareStatement(sql_bid_update);
			
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
			
			
			Date date = null;
			Timestamp tsp = null;
			try {
				date = sdf.parse(time.trim());
				tsp = new Timestamp(date.getTime());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			
			long dt = tsp.getTime();
			
			m_connection.setAutoCommit(false);
			Set<String> sKey = m_bid_map.keySet();
			Iterator<String> sIter = sKey.iterator();
			while(sIter.hasNext())
			{
				String skey = sIter.next();
				BigDecimal bid = m_bid_map.get(skey);
				Byte bidmode = m_bidmode_map.get(skey);
				
				m_update_statement.setString(1, skey);
				m_update_statement.setBigDecimal(2, bid);
				m_update_statement.setByte(3, bidmode);
				m_update_statement.setLong(4, dt);
				//m_update_statement.execute();
				m_update_statement.addBatch();
			}
			
			m_update_statement.executeBatch();
			m_connection.commit();
			m_connection.close();
			
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFoundException setup Error:" + e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println("SQLException setup Error:" + e.getMessage());
			e.printStackTrace();
		} finally {
			DBUtil.close(m_connection, m_update_statement, null);
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long dt = 1362067200000l;
		BidInfo bi = new BidInfo();
		bi.initBidInfo();
		System.out.println("init ok");
//		bi.updateLocalBidInfo(dt);
		System.out.println("update ok");
	}
}
