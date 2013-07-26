package adbi.mapreduce.check;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.HDFSLogWritter;

public class AdCommonStatCheckMapper extends Mapper<LongWritable, Text, Text, Text>{
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatCheckMapper/";
	private static HDFSLogWritter LOG = null;
	private static List<Integer> mDim = new ArrayList<Integer>();
	
	protected void setup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		mDim.clear();
		mDim.add(6);
		LOG.info("AdCommonStatCheckMapper setup!");
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatCheckMapper cleanup!");
		LOG.close();
	}
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, 
	InterruptedException  {
		String allvalue = value.toString().trim();
		String[] fields = allvalue.split("\t");
		if(fields.length != 2)
		{
			LOG.error("format error:" + allvalue);
			return;
		}
		
		String skey = fields[0].trim();
		String[] tags = skey.split("\\^\\^");
		StringBuilder okey = new StringBuilder();
		for(int i = 0;i < mDim.size();++i)
		{
			int idx = mDim.get(i);
			if(idx < 0 || idx >= tags.length)
			{
				LOG.error("dimension index wrong:" + idx + "," + skey + ",skip");
				continue;
			}
			okey.append(tags[idx]);
			okey.append("^^");
		}
		
		if(okey.length() > 0)
			okey.setLength(okey.length()-2);
		
		String svalue = fields[1].trim();
		tags = svalue.split("\\^\\^");
		if(tags.length != 3)
		{
			LOG.error("value lenghth wrong:" + svalue + ",skip");
			return;
		}
		
		
		context.write(new Text(okey.toString()),new Text(tags[0] + "^^" + tags[1]));
	}
}
