package adbi.mapreduce.reachdb;

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

@SuppressWarnings("deprecation")
public class AdReachStatDBInsertTimeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdReachStatDBInsertTimeMapper/";
	private static HDFSLogWritter LOG = null;
	
	public static Connection conn = null;
	public static PreparedStatement stm = null;

	public static long i = 0l;
	public static boolean sucess = true;

	private String dbTime = "";
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
	private SimpleDateFormat ssdf = new SimpleDateFormat("yyyy/MM/dd");
	
	private StringBuffer sql_ad_hour_pv_click = new StringBuffer("insert into ad_hour_reach_pv_click"
			+ "(target, target_definition, time_hour, pv, click, date, adtype) "
			+ "VALUES(?, ?, ?, ?, ?, ?, ?)");
	
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
			
			stm = conn.prepareStatement(sql_ad_hour_pv_click.toString());
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
		LOG.info("AdDBInsertTimeMapper setup!");
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
			LOG.info("AdDBInsertTimeMapper cleanup!");
			LOG.close();
		}
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException{
		String[] tag = null;
		Date date = null;
		Timestamp tsp = null;
		
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
				LOG.warn("Input line format error, key is not 4 fileds(statindex,statdim,target,time,adtype). key:" + splits[0]);
				return;
			}
			
			String target_key = tag[2];
			String time_of_hour = tag[3];
			String adtype = tag[4];
			
			String tmp_tars[] = target_key.split("=");
			if(tmp_tars.length != 2)
			{
				LOG.warn("target key format error, key is not two fileds. key:" + target_key);
				return;
			}
			
			date = sdf.parse(time_of_hour.trim());
			tsp = new Timestamp(date.getTime());
			
			tag = splits[1].split("\\^\\^");
			if(tag.length != 2)
			{
				LOG.warn("Input line format error, value is not 2 fields. value:" + splits[1]);
				return;
			}
			
			String ad_pv = tag[0];
			String ad_click = tag[1];
			
			Date date1 = null;
			Timestamp tsp1 = null;
			try
			{
				date1 = ssdf.parse(dbTime.trim());
				tsp1 = new Timestamp(date1.getTime());
			}catch (ParseException e) {
					e.printStackTrace();
			}
			
			stm.setString(1, tmp_tars[0]);
			stm.setString(2, tmp_tars[1]);
			stm.setLong(3, tsp.getTime());
			stm.setLong(4, Long.parseLong(ad_pv));
			stm.setLong(5, Long.parseLong(ad_click));
			stm.setLong(6, tsp1.getTime());
			stm.setByte(7, Byte.valueOf(adtype));
			stm.addBatch();
			
			i++;
			if (i % 10000 == 0) {
				stm.executeBatch();
			}
		} catch (ParseException e) {
			LOG.error("ParseException map Error:" + e.getMessage() + "::"
					+ value.toString());
			 sucess = false;
			e.printStackTrace();
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
