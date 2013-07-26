package temporary.statuarate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.HDFSLogWritter;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
public class UARateStep2Mapper extends Mapper<Text, LongWritable,Text, LongWritable> {
	
	private final String LogAddr = "/user/aalog/ad_bi/uarate/log_tmp/UARateStep2Mapper/";
	private static HDFSLogWritter LOG = null;
	
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("UARateStep2Mapper setup!");
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);
		LOG.info("UARateStep2Mapper cleanup!");
		LOG.close();
	}
	
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		String skey = key.toString().trim();
		String[] tags = skey.split("\\^\\^");
		if(tags.length != 2)
		{
			LOG.error("key format error:" + skey);
			return;
		}
		
		context.write(new Text("TagNum-" + tags[1]), value);
	}
}



