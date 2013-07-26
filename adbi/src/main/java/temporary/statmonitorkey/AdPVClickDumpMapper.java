package temporary.statmonitorkey;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.data.Tuple;

import sessionlog.mapreduce.BaseMapper;
import sessionlog.op.AdInfoOperation;

import util.ConstData;
import util.HDFSLogWritter;
import util.StringUtil;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
public class AdPVClickDumpMapper extends BaseMapper<Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdPVClickDumpMapper/";
	private static HDFSLogWritter LOG = null;
	
	private final SimpleDateFormat sdf_hour = new SimpleDateFormat("yyyy-MM-dd-HH");
	private List<AdInfoOperation> pvList = null;
	private List<AdInfoOperation> clickList = null;
	
	public static String getProjectionConf()
	{
		return ConstData.CG_FIELDS;
	}
	
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdPVClickDumpMapper setup!");
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);
		LOG.info("AdPVClickDumpMapper cleanup!");
		LOG.close();
	}
	
	@SuppressWarnings("unchecked")
	protected void map(BytesWritable key, Tuple value, Context context) throws IOException, InterruptedException {
		decode(key, value);
		
		//1,get pv data
		String uid = (String)list.get(ConstData.CG_USER);
		pvList = (List<AdInfoOperation>) list.get(ConstData.CG_ADDISPLAY);
		if (pvList != null && pvList.size() != 0) {
			StringBuffer tmp = null;
			for (AdInfoOperation pv : pvList) {
				//ad key
				String ad_key = pv.getMonitorKey();
				if(!ad_key.equals("200ga000c0000000q1c000q1d"))
					continue;
				
				//region,time,type
				String region = StringUtil.split(pv.getRegion());
				long timeStamp = pv.getTimestamp();
				//String timeOfHour = sdf_hour.format(new Date(timeStamp));
				String timeOfHour = sdf_hour.format(new Date(timeStamp*1000));
				String adType = pv.getAdType();
				
				//build cube key
				tmp = new StringBuffer();
				tmp.append(ConstData.DT_ADDISPLAY);
				tmp.append("^^");
				tmp.append(ad_key);
				tmp.append("^^");
				tmp.append(region);
				tmp.append("^^");
				tmp.append(timeOfHour);
				tmp.append("^^");
				tmp.append(adType);
				
				if(uid != null && !uid.trim().equals(""))
					context.write(new Text(uid.trim()), new Text(tmp.toString()));
			}
		}
		
		//2,get click data
		clickList = (List<AdInfoOperation>) list.get(ConstData.CG_ADCLICK);
		if (clickList != null && clickList.size() != 0) {
			StringBuffer tmp = null;
			for (AdInfoOperation pv : clickList) {
				//ad key
				String ad_key = pv.getMonitorKey();
				if(!ad_key.equals("200ga000c0000000q1c000q1d"))
					continue;
				
				//region,time,type
				String region = StringUtil.split(pv.getRegion());
				long timeStamp = pv.getTimestamp();
				//String timeOfHour = sdf_hour.format(new Date(timeStamp));
				String timeOfHour = sdf_hour.format(new Date(timeStamp*1000));
				String adType = pv.getAdType();
				
				//build cube key
				tmp = new StringBuffer();
				tmp.append(ConstData.DT_ADCLICK);
				tmp.append("^^");
				tmp.append(ad_key);
				tmp.append("^^");
				tmp.append(region);
				tmp.append("^^");
				tmp.append(timeOfHour);
				tmp.append("^^");
				tmp.append(adType);
				
				if(uid != null && !uid.trim().equals(""))
					context.write(new Text(uid.trim()), new Text(tmp.toString()));
			}
		}
	}
}



