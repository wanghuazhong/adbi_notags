package adbi.mapreduce.data;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import util.ConstData;

import com.sohu.ADRD.AudienceTargeting.type.basic.UserWritable;
import com.sohu.ADRD.AudienceTargeting.type.basic.TagsWritable;
import com.sohu.ADRD.AudienceTargeting.util.ExceptionUtil;
import com.sohu.ADRD.AudienceTargeting.util.HDFSLogWritter;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
@SuppressWarnings("deprecation")
public class UserTagsFormatMapper extends MapReduceBase implements Mapper<UserWritable, TagsWritable, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/UserTagsFormatMapper/";
	private static HDFSLogWritter LOG;
	
	private Hashtable<String,String> userTagsWhiteList = new Hashtable<String,String>();
	
	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		TaskAttemptID atmpId = TaskAttemptID.forName(conf.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		try
		{
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			if(cacheFiles != null && cacheFiles.length > 0)
			{
				String line;
				String[] tokens;
				
				//user tags white list
				BufferedReader joinReader = new BufferedReader(new FileReader("user_tags_white_list.txt"));
				try{
					while((line = joinReader.readLine()) != null){
						tokens = line.split("\\t");
						if(tokens.length != 2)
						{
							LOG.error("usertags fields not equals 2:" + line);
							continue;
						}
						userTagsWhiteList.put(tokens[0], tokens[1]);
						LOG.info("usertags white list:" + tokens[0] + "<=>" + tokens[1]);
					}
				}finally{
					joinReader.close();
				}
			}
		}catch (IOException e){
			LOG.info("Exception reading distributedcache:" + e);
		}
		LOG.info("UserTagsFormatMapper setup!");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("UserTagsFormatMapper cleanup!");
		LOG.close();
	}

	@Override
	public void map(UserWritable key, TagsWritable value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		try {
			long tag_mask = 0xffffffffffff0000l;
			
			String user = key.getuserid();
			if(user == null)
				return;
			user = user.trim();
			if(user.equals(""))
				return;
			
			List<Long> tagList = value.gettags_long();
			StringBuffer tags = new StringBuffer();
			tags.append(ConstData.DT_USERTAG);
			tags.append("^^");
			for(java.util.Iterator<Long> iter = tagList.iterator();iter.hasNext();)
			{
				long tagall = iter.next();
				long tag = tagall&tag_mask;
				if(userTagsWhiteList.containsKey(String.valueOf(tag)))
				{
					tags.append(tag);
					tags.append(",");
				}
			}
			context.collect(new Text(user), new Text(tags.toString()));
			
		} catch(Exception ex) {
			LOG.error(ExceptionUtil.toString(ex));
		}
	}	
}


