package adbi.mapreduce.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import util.ConstData;
import util.HDFSLogWritter;


@SuppressWarnings("deprecation")
public class AdPVClickTagJoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdPVClickTagJoinReducer/";
	private static HDFSLogWritter LOG = null;
	
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		TaskAttemptID atmpId = TaskAttemptID.forName(conf.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		LOG.info("AdPVClickTagJoinReducer setup!");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("AdPVClickTagJoinReducer cleanup!");
		LOG.close();
	}

	@Override
	public void reduce(Text key, Iterator<Text> iter,
			OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		//save user logs and tags to a list, meanwhile merge user's all taglist, no deduplication
		long userTagListCount = 0;
		StringBuilder tags = new StringBuilder();
		
		List<String> tagLogList = new ArrayList<String>();
		String tagLog = "";
		while(iter.hasNext())
		{
			tagLog = ((Text)iter.next()).toString().trim();
			if(tagLog.equals(""))
				continue;
			
			tagLogList.add(tagLog);
			if(tagLog.startsWith(ConstData.DT_USERTAG))
			{
				tags.append(tagLog.substring(ConstData.DT_USERTAG.length() + "^^".length()));
				++userTagListCount;
			}
		}
		
		if(userTagListCount > 1)
		{
			LOG.warn("There is something wrong, for user[" + key.toString() + "] has more than one tagList[" 
					+ userTagListCount + "], merge them");
		}
		
		//add tags to user
		String log = "";
		Iterator<String> titer1 = tagLogList.iterator();
		while(titer1.hasNext())
		{
			log = titer1.next().trim();
			if(log.equals("") || log.startsWith(ConstData.DT_USERTAG))
				continue;
			
			String tmp = log + "^^" + tags.toString();
			
			if(key != null && !key.toString().trim().equals(""))
			{
				output.collect(key, new Text(tmp));
			}
		}
	}
}
