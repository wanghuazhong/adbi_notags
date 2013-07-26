package adbi.mapreduce.commonstat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.ConstData;
import util.HDFSLogWritter;

public class AdCommonStatReducer extends Reducer<Text, Text, Text, Text> {
	
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatReducer/";
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
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatReducer cleanup!");
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
			revenue = revenue.add(new BigDecimal(col[0]));
			total += Long.valueOf(col[1]);
		}	
		
		
		String skey = key.toString();
		String[] tags = skey.split("\\^\\^");
		String statindex = tags[1];
		String statdim = tags[2];
		if(statindex.equalsIgnoreCase(ConstData.ID_VISIT))
		{
			if(statdim.equalsIgnoreCase(ConstData.DM_USER))
			{
				if(tags.length != 7)
				{
					LOG.warn("key not 7 fileds(datatype, statindex, statdim, indexvalue, adKeyStatus, dimvalue, adtype):" + skey);
					return;
				}
				
				String datatype = tags[0];
				String data = tags[1] + "^^" + tags[2] + "^^" + tags[3] + "^^" + tags[4] + "^^" + tags[5] + "^^" + tags[6];
				
				context.write(new Text(data), new Text(datatype + "^^" + String.valueOf(total) + "^^" + String.valueOf(revenue)));
			}
			else if(statdim.equalsIgnoreCase(ConstData.DM_REGION)){
				if(tags.length != 7){
					LOG.warn("key not 7 fileds(datatype,statindex,statdim,indexvalue,adKeyStatus,dimvalue,adtype):" + skey);
					return;
				}
				String datatype = tags[0];
				String data = tags[1] + "^^" + tags[2] + "^^" + tags[3] + "^^" + tags[4] + "^^" + tags[5] + "^^" + tags[6];
				
				context.write(new Text(data), new Text(datatype + "^^" + String.valueOf(total)+ "^^" + String.valueOf(revenue)));
			}
			else
			{
				if(tags.length != 8)
				{
					LOG.warn("key not 8 fileds(datatype, statindex, statdim, indexvalue,adKeyStatus, dimvalue, adpos,adtype):" + skey);
					return;
				}
				
				String datatype = tags[0];
				String data = tags[1] + "^^" + tags[2] + "^^" + tags[3] + "^^" + tags[4] + "^^" + tags[5] + "^^" + tags[6] + "^^" + tags[7];
				
				context.write(new Text(data), new Text(datatype + "^^" + String.valueOf(total)+ "^^" + String.valueOf(revenue)));
			}
		}
	}
}
