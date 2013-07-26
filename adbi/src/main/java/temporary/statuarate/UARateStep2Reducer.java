package temporary.statuarate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.HDFSLogWritter;

public class UARateStep2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private final String LogAddr = "/user/aalog/ad_bi/uarate/log_tmp/UARateStep2Reducer/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException{
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("UARateStep2Reducer setup!");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		LOG.info("UARateStep2Reducer cleanup!");
		LOG.close();
	}
	@Override
	protected void reduce(Text key,  Iterable<LongWritable> values, Context context) throws IOException, 
	InterruptedException{
		long total = 0l;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()){
			iter.next();
			++total;
		}
		
		context.write(key, new LongWritable(total));
	}
}
