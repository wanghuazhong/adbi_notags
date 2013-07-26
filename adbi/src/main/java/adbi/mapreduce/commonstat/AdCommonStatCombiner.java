package adbi.mapreduce.commonstat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.HDFSLogWritter;

public class AdCommonStatCombiner extends Reducer<Text, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatCombiner/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);

		LOG.info("AdCommonStatCombiner setup!");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatCombiner cleanup!");
		LOG.close();
	}
	@Override
	protected void reduce(Text key,  Iterable<Text> values, Context context) throws IOException, 
	InterruptedException{
		// TODO Auto-generated method stub
//		double revenue = 0.0;
		long total = 0l;
		
		BigDecimal revenue = new BigDecimal(0.0);
		
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()){
			String value = iter.next().toString();
			String[] col = value.split("\\^\\^");
			if (col.length != 2) {
				LOG.error("value format error, fields not 2(revenue,count):"
						+ value + ", length:" + col.length);
				return;
			}
//			revenue += Double.valueOf(col[0]);
			total += Long.valueOf(col[1]);
			revenue = revenue.add(new BigDecimal(col[0]));
		}	
		context.write(key, new Text(String.valueOf(revenue) + "^^" + String.valueOf(total)));
	}
}
