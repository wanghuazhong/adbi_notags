package adbi.mapreduce.commonstat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

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
public class AdCommonStatJoinReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatJoinReducer/";
	private static HDFSLogWritter LOG = null;

	@Override
	public void configure(JobConf conf) {
		// TODO Auto-generated method stub
		TaskAttemptID atmpId = TaskAttemptID
				.forName(conf.get("mapred.task.id"));
		LOG = new HDFSLogWritter(LogAddr
				+ "task"
				+ HDFSLogWritter.getFullNumber(atmpId.getTaskID().getId(), 4)
				+ "_"
				+ HDFSLogWritter.getFullNumber(
						(int) (System.currentTimeMillis() % 10000), 4), conf);
		LOG.info("AdCommonStatJoinReducer setup!");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatJoinReducer cleanup!");
		LOG.close();
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> context, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub

		String[] tags = key.toString().split("\\^\\^");
		if (tags.length < 5) {
			LOG.error("key format error, fields less than 4(statindex, statdim, indexvalue, adKeyStatus, dimvalue[option], adpos[option], adtype):"
					+ key.toString() + ", lenghth:" + tags.length);
			return;
		}

		String statdim = tags[1];
		if (statdim.equalsIgnoreCase(ConstData.DM_USER)) {
			if (tags.length != 5) {
				LOG.error("statdim:"
						+ statdim
						+ " key format error, fields not 5(statindex, statdim, indexvalue, adKeyStatus, adtype):"
						+ key.toString() + ", lenghth:" + tags.length);
				return;
			}

			long total = 0l;
			while (values.hasNext()) {
				values.next();
				++total;
			}

			context.collect(key, new Text(String.valueOf(total)));
		} else {
			if (tags.length < 6) {
				LOG.error("statdim:"
						+ statdim
						+ " key format error, fields less than 5(statindex, statdim, indexvalue, adKeyStatus, dimvalue, adpos[option],adtype):"
						+ key.toString() + ", lenghth:" + tags.length);
				return;
			}

			String pvCount, clickCount;
			pvCount = clickCount  =null;
//			double revenue = 0.0;
			BigDecimal revenue = new BigDecimal(0.0);

			int size = 0;
			String[] cols = null;
			while (values.hasNext()) {
				String value = values.next().toString();
				cols = value.split("\\^\\^");
				if (cols.length != 3) {
					LOG.error("value format error, fields not 3(datatype,count,revenue):"
							+ value + ", length:" + cols.length);
					return;
				}

				String datatype = cols[0];
				String count = cols[1];
//				revenue += Double.valueOf(cols[2]);
				revenue = revenue.add(new BigDecimal(cols[2]));
				if (datatype.equalsIgnoreCase(ConstData.DT_ADCLICK))
					clickCount = count;
				else if (datatype.equalsIgnoreCase(ConstData.DT_ADDISPLAY))
					pvCount = count;
				else {
					LOG.error("only click and pv datatype should be there:"
							+ datatype);
					return;
				}
				++size;
			}

			if (pvCount == null)
				pvCount = "0";

			if (clickCount == null)
				clickCount = "0";

			if (size > 3) {
				LOG.error("more than 3, impossible for pv and click join:"
						+ size);
				return;
			}

			context.collect(key, new Text(pvCount + "^^" + clickCount + "^^" + String.valueOf(revenue)));
		}
	}
}
