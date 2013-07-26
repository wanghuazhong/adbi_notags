package adbi.mapreduce.check;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.HDFSLogWritter;

public class AdCommonStatCheckReducer extends Reducer<Text, Text, Text, Text>  {
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatCheckReducer/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdCommonStatCheckReducer setup!");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatCheckReducer cleanup!");
		LOG.close();
	}
	@Override
	protected void reduce(Text key,  Iterable<Text> values, Context context) throws IOException, 
	InterruptedException{
		long pv_total,click_total;
		pv_total = click_total = 0l;
		String lt;
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()){
			lt = iter.next().toString().trim();
			if(lt.equals(""))
				continue;
			
			String[] tags = lt.split("\\^\\^");
			if(tags.length != 2)
			{
				LOG.error("count field format error" + lt);
				continue;
			}
			
			pv_total += Long.valueOf(tags[0]);
			click_total += Long.valueOf(tags[1]);
		}
		context.write(key,new Text(pv_total + "\t" + click_total));
	}
}
