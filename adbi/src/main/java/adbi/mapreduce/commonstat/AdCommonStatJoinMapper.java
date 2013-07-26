package adbi.mapreduce.commonstat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import util.ConstData;
import util.HDFSLogWritter;

@SuppressWarnings("deprecation")
public class AdCommonStatJoinMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>  {
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatJoinMapper/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		TaskAttemptID atmpId = TaskAttemptID.forName(conf.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdCommonStatJoinMapper setup!");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatJoinMapper cleanup!");
		LOG.close();
	}
	
	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		String statinfokey = key.toString();
		if(statinfokey == null)
			return;
		statinfokey = statinfokey.trim();
		if(statinfokey.equals(""))
			return;
		
		String statinfovalue = value.toString();
		if(statinfovalue == null)
			return;
		statinfovalue = statinfovalue.trim();
		if(statinfovalue.equals(""))
			return;
		
		String[] cols1 = statinfovalue.split("\\^\\^");
		if(cols1.length != 3)
		{
			LOG.warn("statinfovalue format error, value is not 2[datatype,count,revenue] fileds. value:"
					+  statinfovalue + " fields count:" + cols1.length);
			return;
		}
		
		String[] cols0 = statinfokey.split("\\^\\^");
		if(cols0.length < 3)
		{
			LOG.warn("statinfokey format error, value is less than 3[statindex,statdim,indexvalue] fileds. key:"
					+  statinfokey + " fields count:" + cols0.length);
			return;
		}
		
		String statindex = cols0[0];
		String statdim = cols0[1];
		String indexvalue = cols0[2];
		String adKeyStatus = cols0[3];
		String adtype = "";
		if(statindex.equals(ConstData.ID_VISIT))
		{
			if(statdim.equalsIgnoreCase(ConstData.DM_USER))
			{
				if(cols0.length != 6)
				{
					LOG.warn("statinfokey format error, value is not 5[statindex, statdim, indexvalue, adKeyStatus, dimvalue, adtype] fileds. value:"
							+  statinfokey + " fields count:" + cols0.length);
					return;
				}
				adtype = cols0[5];
			}
			else if(statdim.equalsIgnoreCase(ConstData.DM_REGION)){
				if(cols0.length != 6){
					LOG.warn("statinfokey format error, value is not 5[statindex, statdim, indexvalue, adKeyStatus, dimvalue, adtype] fileds. value:"
							+  statinfokey + " fields count:" + cols0.length);
					return;
				}
				adtype = cols0[5];
			}
			else
			{
				if(cols0.length != 7)
				{
					LOG.warn("statinfokey format error, value is not 6[statindex, statdim, indexvalue, adKeyStatus, dimvalue, adpos,adtype] fileds. value:"
							+  statinfokey + " fields count:" + cols0.length);
					return;
				}
				adtype = cols0[6];
			}
			
			if(statdim.equalsIgnoreCase(ConstData.DM_USER))
				context.collect(new Text(statindex + "^^" + statdim + "^^" + indexvalue + "^^" + adKeyStatus + "^^" + adtype), new Text(statinfovalue));
			else
				context.collect(new Text(statinfokey), new Text(statinfovalue));
		}
	}
}
