package adbi.mapreduce.visitdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import util.DBUtil;
import util.HDFSLogWritter;
import util.MonitorKeyParser;


@SuppressWarnings("deprecation")
public class AdVisitStatDBInsertUVMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdVisitStatDBInsertUVMapper/";
	private static HDFSLogWritter LOG = null;
	
	public static Connection conn = null;
	public static PreparedStatement stm = null;

	public static long i = 0l;
	public static boolean sucess = true;

	private String dbTime = "";
	
	private MonitorKeyParser keyParser = new MonitorKeyParser();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

	private StringBuffer sql_ad_day_uv = new StringBuffer("insert into ad_day_uv"
			+ "(ad_key, camp_source, advertiser, campaign, adgroup, line, material, date, uv, adtype) "
			+ "VALUES(?, ?, ?, ?,?, ?, ?, ?,?, ?)");
	
	@Override
	public void configure(JobConf context) {
		TaskAttemptID atmpId = TaskAttemptID.forName(context.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, context);
		dbTime = context.get("mapred.db_time", "2012/12/23");
		
		try {
			DBUtil util = new DBUtil();
			conn = util.getConnection();
			conn.setAutoCommit(false);
			
			stm = conn.prepareStatement(sql_ad_day_uv.toString());
			
		} catch (ClassNotFoundException e) {
			LOG.error("ClassNotFoundException setup Error:" + e.getMessage());
			sucess = false;
			e.printStackTrace();
		} catch (SQLException e) {
			LOG.error("SQLException setup Error:" + e.getMessage());
			sucess = false;
			e.printStackTrace();
		}
		LOG.info("dbTime:" + dbTime);
		LOG.info("AdDBInsertUVMapper setup!");
	}

	@Override
	public void close() throws IOException {
		try {			
			if (sucess == true) {
				stm.executeBatch();
				conn.commit();
			} else {
				conn.rollback();
				throw new NullPointerException("------>>>>>>>>>>conn.rollback()!");
			}
			LOG.info("sucess:" + sucess + ",total:" + i);
		} catch (SQLException e) {
			LOG.error("SQLException Error in cleanup:" + e.getMessage());
			e.printStackTrace();
		} catch(NullPointerException e) {
			LOG.error("NullPointerException Error in cleanup:" + e.getMessage());
			e.printStackTrace();
		}
		finally {
			try {
				conn.setAutoCommit(true);
				DBUtil.close(conn, stm, null);
			} catch (SQLException e) {
				LOG.error("SQLException Error setAutoCommit cleanup:"
						+ e.getMessage());
				e.printStackTrace();
			}
			LOG.info("AdDBInsertUVMapper cleanup!");
			LOG.close();
		}
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException{
		Date date = null;
		Timestamp tsp = null;
		String[] tag = null;
		try {
			String[] splits = value.toString().split("\\t");
			if(splits.length != 2)
			{
				LOG.warn("Input line format error, line is not two fileds. line:" + value.toString());
				return;
			}
			
			tag = splits[0].split("\\^\\^");
			if(tag.length != 4)
			{
				LOG.warn("Input line format error, key is not 4 fileds(statindex,statdim,adkey,adtype). key:" + splits[0]);
				return;
			}
			
			String ad_key = tag[2];
			String adtype = tag[3];
			//LOG.info("ADTYPE:" + adtype);
			
			String uv = splits[1];
		
			try
			{
				date = sdf.parse(dbTime.trim());
				tsp = new Timestamp(date.getTime());
			}catch (ParseException e) {
					LOG.warn("ParseException:"+e.getMessage());
					e.printStackTrace();
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
			
			i++;
			if (i % 10000 == 0) {
				stm.executeBatch();
			}
		} catch (SQLException e) {
			LOG.error("SQLException map Error:" + e.getMessage() + "::"
					+ value.toString());
			 sucess = false;
			e.printStackTrace();
		} catch(NullPointerException e) {
			LOG.error("NullPointerException map Error:" + e.getMessage() + "::"
					+ value.toString());
			 sucess = false;
			e.printStackTrace();
		}
	}
}
