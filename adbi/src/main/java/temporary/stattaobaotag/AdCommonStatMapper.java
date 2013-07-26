package temporary.stattaobaotag;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.ConstData;

import com.sohu.ADRD.AudienceTargeting.type.basic.UserWritable;
import com.sohu.ADRD.AudienceTargeting.type.basic.TagsWritable;
import com.sohu.ADRD.AudienceTargeting.util.ExceptionUtil;
import com.sohu.ADRD.AudienceTargeting.util.FloatUtil;
import com.sohu.ADRD.AudienceTargeting.util.HDFSLogWritter;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
public class AdCommonStatMapper extends Mapper<UserWritable, TagsWritable, Text, LongWritable> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/UserTagsFormatMapperTMP/";
	private static HDFSLogWritter LOG;
	
	private static LongWritable ONE = new LongWritable(1);
	private Hashtable<String,String> userTagsWhiteList = new Hashtable<String,String>();
	
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
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
				BufferedReader joinReader = new BufferedReader(new FileReader("tbk_user_tags_white_list.txt"));
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
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		LOG.info("UserTagsFormatMapper cleanup!");
		LOG.close();
	}

	public void map(UserWritable key, TagsWritable value, Context context) throws IOException {
		// TODO Auto-generated method stub
		try {
			long type_mask = 0xffff000000000000l;
			long tag_mask = 0xffffffffffff0000l;
			
			String user = key.getuserid();
			if(user == null)
				return;
			user = user.trim();
			if(user.equals(""))
				return;
			
			FloatUtil fu = new FloatUtil();
			List<Long> tagList = value.gettags_long();
			StringBuffer tags = new StringBuffer();
			tags.append(ConstData.DT_USERTAG);
			tags.append("^^");
			for(java.util.Iterator<Long> iter = tagList.iterator();iter.hasNext();)
			{
				long tagall = iter.next();
				long type = tagall&type_mask;
				if(type != (6l<<48))
					continue;
				
				long tag = tagall&tag_mask;
				if(!userTagsWhiteList.containsKey(String.valueOf(tag)))
					continue;
				
				double weight = fu.getWeight(tagall);
				if(weight < 0.8)
					continue;
				
				context.write(new Text(user + "^^" + "taobao"), ONE);
			}
		} catch(Exception ex) {
			LOG.error(ExceptionUtil.toString(ex));
		}
	}	
}


