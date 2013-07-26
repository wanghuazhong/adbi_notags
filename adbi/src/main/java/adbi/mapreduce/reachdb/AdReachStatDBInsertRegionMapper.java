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
public class AdReachStatDBInsertRegionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdReachStatDBInsertRegionMapper/";
	private static HDFSLogWritter LOG = null;
	
	public static Connection conn = null;
	public static PreparedStatement stm = null;

	public static long i = 0l;
	public static boolean sucess = true;

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	
	private String dbTime = "";
	
	private StringBuffer sql_ad_city_pv_click = new StringBuffer("insert into ad_city_reach_pv_click"
			+ "(target, target_definition, country, province, city, pv, click, date, adtype) "
			+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
	
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
			
			stm = conn.prepareStatement(sql_ad_city_pv_click.toString());
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
		LOG.info("AdDBInsertRegionMapper setup!");
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
			LOG.info("AdDBInsertRegionMapper cleanup!");
			LOG.close();
		}
	}
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException{
		String[] tag = null;
		
		try {
			String[] splits = value.toString().split("\\t");
			if(splits.length != 2)
			{
				LOG.warn("Input line format error, line is not two fileds. line:" + value.toString());
				return;
			}
			
			tag = splits[0].split("\\^\\^");
			if(tag.length != 5)
			{
				LOG.warn("Input line format error, key is not 5 fileds(statindex,statdim,adkey,region,adtype). key:" + splits[0]);
				return;
			}
			
			String target_key = tag[2];
			String region = tag[3];
			String adtype = tag[4];
			
			String tmp_tars[] = target_key.split("=");
			if(tmp_tars.length != 2)
			{
				LOG.warn("target key format error, key is not two fileds(target,definition). key:" + target_key);
				return;
			}
			
			String country, province, city;
			country = province = city = "-";
			
			if(!region.equalsIgnoreCase("unkn-unknow"))
			{
				String fields[] = region.split("-");
				if(fields.length == 1)
				{
					country = fields[0];
				}
				else if(fields.length == 2)
				{
					if(fields[1].length() < 6)
					{
						LOG.error("Region code error, lengh is less than 6. Region code:" + fields[1]);
						return;
					}
					country = fields[1].substring(0, 2);
					province = fields[1].substring(2,4);
					city = fields[1].substring(4);
				}
				else
				{
					LOG.error("Region code error, fields length is neither 1 nor 2. Region code:" + region);
					return;
				}
			}
			
			tag = splits[1].split("\\^\\^");
			if(tag.length != 2)
			{
				LOG.warn("Input line format error, value is not two fields. value:" + splits[1]);
				return;
			}
			
			String ad_pv = tag[0];
			String ad_click = tag[1];

			Date date = null;
			Timestamp tsp = null;
			try
			{
				date = sdf.parse(dbTime.trim());
				tsp = new Timestamp(date.getTime());
			}catch (ParseException e) {
					e.printStackTrace();
			}
			
			stm.setString(1, tmp_tars[0]);
			stm.setString(2, tmp_tars[1]);
			stm.setString(3, country);
			stm.setString(4, province);
			stm.setString(5, city);
			stm.setLong(6, Long.parseLong(ad_pv));
			stm.setLong(7, Long.parseLong(ad_click));
			stm.setLong(8, tsp.getTime());
			stm.setByte(9, Byte.valueOf(adtype));
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
