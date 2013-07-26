package temporary.statuarate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.data.Tuple;

import com.sohu.ADRD.AudienceTargeting.io.redis.UserAttributeReader;
import com.sohu.ADRD.AudienceTargeting.io.redis.res;

import sessionlog.mapreduce.BaseMapper;
import util.ConstData;
import util.HDFSLogWritter;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 *
 */
public class UARateStep1Mapper extends BaseMapper<Text, LongWritable> {
	
	private final String LogAddr = "/user/aalog/ad_bi/uarate/log_tmp/UARateStep1Mapper/";
	private static HDFSLogWritter LOG = null;
	private static UserAttributeReader uaReader = null;
	private static LongWritable ONE = new LongWritable(1l);
	
	public static String getProjectionConf()
	{
		return ConstData.CG_FIELDS;
	}
	
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr + "task" + HDFSLogWritter.getFullNumber(context.getTaskAttemptID().getTaskID().getId(), 4)
				+ "_" + HDFSLogWritter.getFullNumber((int)(System.currentTimeMillis() % 10000), 4)
				, conf);
		try {
			uaReader = new UserAttributeReader("192.168.230.95",9090);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("UARateStep1Mapper setup!");
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		super.cleanup(context);
		LOG.info("UARateStep1Mapper cleanup!");
		uaReader.close();
		LOG.close();
	}
	
	protected void map(BytesWritable key, Tuple value, Context context) throws IOException, InterruptedException {
		decode(key, value);
		
		String uid = (String)list.get(ConstData.CG_USER);
		if(uid == null)
		{
			LOG.error("PVClick uid is null");
			return;
		}
		uid = uid.trim();
		if(uid.equals(""))
		{
			LOG.error("PVClick uid is empty");
			return;
		}
		
		res result = null;
		try {
			result = uaReader.getTags(uid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long type_mask = 0xffff000000000000l;
		int count = 0;
		if(result != null)
		{
			count = result.oval.size();
			
			for(int i = 0;i < result.oval.size();++i)
			{
				long tagAll = result.oval.get(i);
				long type = tagAll&type_mask;
				if(type == (6l<<48))
					context.write(new Text(uid + "^^TBKW-" + count), ONE);
				else if(type == (7l<<48))
					context.write(new Text(uid + "^^TBCT-" + count), ONE);
			}
		}
		
		context.write(new Text(uid + "^^" + count), ONE);
	}
}



