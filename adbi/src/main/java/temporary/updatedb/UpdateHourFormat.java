package temporary.updatedb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import util.ConstData;
import util.DBUtil;

public class UpdateHourFormat {
	//sql data structure
	private static Connection m_connection = null;
	private static Statement m_statement = null;
	private static ResultSet m_resultset = null;
	
	private static String sql_hour = "select distinct time_hour,date from ad_hour_pv_click_new";
	private static String sql_new_hour = "update ad_hour_pv_click_new set time_hour = ? where time_hour = ? and date = ?";
	
	/**
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DBUtil util = new DBUtil();
			m_connection = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			m_statement = m_connection.createStatement();
			m_resultset = m_statement.executeQuery(sql_hour);
			
			Connection conn = util.getConnection(ConstData.ADPVINSIGHT_SERVER, ConstData.ADPVINSIGHT_PORT, ConstData.ADPVINSIGHT_DB, 
					ConstData.ADPVINSIGHT_USER, ConstData.ADPVINSIGHT_PASSWORD);
			PreparedStatement stmt = conn.prepareStatement(sql_new_hour);
			//conn.setAutoCommit(false);
			
			while(m_resultset.next())
			{
				try
				{
					Long hour = m_resultset.getLong(1);
					Long dd = m_resultset.getLong(2);
					
					if(hour >=24)
					{
						Date date = new Date(hour);
						System.out.println("year:" + (date.getYear() + 1900) + "month:" + date.getMonth() + "date:" + date.getDate() 
								+ "hour:" + date.getHours() + "minutes:" + date.getMinutes() + "seconds:" + date.getSeconds());
						stmt.setLong(1, date.getHours());
						stmt.setLong(2, hour);
						stmt.setLong(3, dd);
						//stmt.addBatch();
						stmt.execute();
					}
				}catch (SQLException e)
				{
					e.printStackTrace();
					System.out.println(e.getMessage());
					continue;
				}
			}
			//stmt.executeBatch();
			//conn.commit();
			conn.close();
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
