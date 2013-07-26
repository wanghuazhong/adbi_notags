package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DBUtil {
	private String jdbcDriverClassName = "org.gjt.mm.mysql.Driver";
	//测试库
//  private String jdbcConnectionUrl = "jdbc:mysql://192.168.110.29:3306/sohu_ad_pvinsight?useUnicode=true&characterEncoding=UTF-8";
//	private String jdbcUsername = "root";
//	private String jdbcPassword = "1234";
	//线上库
	private String jdbcConnectionUrl = "jdbc:mysql://10.16.17.67:3306/sohu_ad_pvinsight?useUnicode=true&characterEncoding=UTF-8";
	private String jdbcUsername = "adpv";
	private String jdbcPassword = "sohuadpv!(0#!";
	

	private String getConnectString(String ip, String port,String db){
		String res = "jdbc:mysql://" + ip + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=UTF-8";
		return res;
	}
	
	public Connection getConnection() throws ClassNotFoundException,
			SQLException {
		Connection conn;
		Class.forName(jdbcDriverClassName);
		conn = DriverManager.getConnection(jdbcConnectionUrl, jdbcUsername,jdbcPassword);
		return conn;
	}
	
	public Connection getConnection(String ip, String port, String db,String user,String password) throws ClassNotFoundException,
	SQLException {
		jdbcConnectionUrl = getConnectString(ip,port,db);
		jdbcUsername = user;
		jdbcPassword = password;
		return getConnection();
		}
	
	public Map<String, Long> queryDB(String sql) throws ClassNotFoundException, SQLException{
		Connection conn = null;
		PreparedStatement stm = null;
		ResultSet rs = null;
		try {
			conn = getConnection();

			stm = (PreparedStatement) conn.prepareStatement(sql);
			rs = stm.executeQuery();
			Map<String, Long> map = new HashMap<String, Long>();
			while (rs.next()) {
				map.put(String.valueOf((rs.getString("VALUE").trim())), Long.valueOf((rs.getLong("id"))));
			}
			return map;
		} catch (ClassNotFoundException e) {
			throw e;
		} catch (SQLException e) {
			 throw e;
		} finally {
			close(conn, stm, rs);
		}
	}

	public static void close(Connection conn, PreparedStatement stm, ResultSet rs) {
		try {
			if (conn != null) {
				conn.close();
			}
			if ( stm!= null) {
				stm.close();
			}
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			System.out
					.println("Connection Close Error:--------------------------------");
			e.printStackTrace();
		}
	}
	
	public static void close(Connection conn, Statement stm, ResultSet rs) {
		try {
			if (conn != null) {
				conn.close();
			}
			if ( stm!= null) {
				stm.close();
			}
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			System.out
					.println("Connection Close Error:--------------------------------");
			e.printStackTrace();
		}
	}
}
