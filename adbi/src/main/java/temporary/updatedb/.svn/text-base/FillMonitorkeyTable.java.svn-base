package temporary.updatedb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import util.ConstData;
import util.DBUtil;
import util.MonitorKeyParser;

public class FillMonitorkeyTable {

	//sql data structure
	private static Connection m_connection = null;
	private static Statement m_statement = null;
	private static ResultSet m_resultset = null;
	
	private static String sql_monitorykeys = "select distinct ad_key from ad_day_pv_click";
	private static String sql_fill_monitory_key = "insert into monitor_key values(?,?,?,?,?,?,?)";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DBUtil util = new DBUtil();
			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			m_statement = m_connection.createStatement();
			m_resultset = m_statement.executeQuery(sql_monitorykeys);
			
			Connection conn = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			PreparedStatement stmt = conn.prepareStatement(sql_fill_monitory_key);
			conn.setAutoCommit(false);
			
			MonitorKeyParser parser = new MonitorKeyParser();
			while(m_resultset.next())
			{
				String key = m_resultset.getString(1);
				parser.set_monitor_key(key);
				if(!parser.parseMonitorKey())
				{
					System.err.println("parse false");
					continue;
				}
				stmt.setString(1, key);
				stmt.setString(2, String.valueOf(parser.get_source_id()));
				stmt.setInt(3, (int)parser.get_advertiser_id());
				stmt.setInt(4, (int)parser.get_campaign_id());
				stmt.setInt(5, (int)parser.get_adgroup_id());
				stmt.setInt(6, (int)parser.get_line_id());
				stmt.setInt(7, (int)parser.get_material_id());
				stmt.addBatch();
			}
			stmt.executeBatch();
			conn.commit();
			
			m_connection.close();
			
		} catch (ClassNotFoundException e) {
			System.err.println("ClassNotFoundException setup Error:" + e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			System.err.println("SQLException setup Error:" + e.getMessage());
			e.printStackTrace();
		}
		
		System.out.println("ok");
	}

}
