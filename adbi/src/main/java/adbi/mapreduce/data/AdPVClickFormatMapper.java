package adbi.mapreduce.data;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import util.HDFSLogWritter;


/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
@SuppressWarnings("deprecation")
public class AdPVClickFormatMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdPVClickFormatMapper/";
	
	private static HDFSLogWritter LOG = null;
	
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		TaskAttemptID atmpId = TaskAttemptID.forName(conf.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdPVClickFormatMapper setup!");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("AdPVClickFormatMapper cleanup!");
		LOG.close();
	}

	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		if(key == null)
			return;
		if(key.toString().trim().equals(""))
			return;
		
		context.collect(key, value);	
	}
	
}



