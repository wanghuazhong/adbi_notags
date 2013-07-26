package temporary.stattaobaotag;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.HDFSLogWritter;

public class AdCommonStatReducer extends Reducer<Text, LongWritable, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatReducerTMP/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdCommonStatReducer setup!");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		LOG.info("AdCommonStatReducer cleanup!");
		LOG.close();
	}
	@Override
	protected void reduce(Text key,  Iterable<LongWritable> values, Context context) throws IOException, 
	InterruptedException{
		// TODO Auto-generated method stub
		long total = 0l;
		LongWritable lt;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()){
			lt = iter.next();
			total += lt.get();
		}
		
		context.write(new Text("taobao"), new Text(String.valueOf(total)));
	}
}
