package adbi.mapreduce.data;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.data.Tuple;

import sessionlog.mapreduce.BaseMapper;
import sessionlog.op.AdInfoOperation;

import util.ConstData;
import util.HDFSLogWritter;

/**
 * 
 * @version 1.0.0
 * @author mafeichao
 * 
 */
public class AdPVClickDumpMapper extends BaseMapper<Text, Text> {

	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdPVClickDumpMapper/";
	private static HDFSLogWritter LOG = null;

	private final SimpleDateFormat sdf_hour = new SimpleDateFormat(
			"yyyy-MM-dd-HH");
	private List<AdInfoOperation> pvList = null;
	private List<AdInfoOperation> clickList = null;

	public static String getProjectionConf() {
		return ConstData.CG_FIELDS;
	}

	public double calculatePrice(String bidType, String bidType2,
			double bidPrice, double bidPrice2, double ecpm, double ecpm2,
			String priceType, String recordType) {
		double revenue = 0.0;
		//展示(display)日志
		if(ConstData.DT_ADDISPLAY.equals(recordType)){
			//按第一高价(price1)计费
			if("price1".equals(priceType)){
				//第一高价是cpm
				if("1".equals(bidType)){
					revenue = bidPrice / 100000.0;
				}
			}
			//按第二高价(price2)计费
			if("price2".equals(priceType)){
				//两个价格的计费方式相同,并且都是cpm
				if("1".equals(bidType) && "1".equals(bidType2)){
					revenue = bidPrice2 / 100000.0;
				}
				//最高出价是cpm,第二高出价是cpc
				if("1".equals(bidType) && "2".equals(bidType2)){
					revenue = ecpm2 / 100000.0;
				}
				//最高出价是cpc,第二高出价是cpm
				//两个出价都是cpc
				//这两种方式的产生的费用都在点击日志(click)里计算
			}
		}
		//点击(click)日志
        if(ConstData.DT_ADCLICK.equals(recordType)){
			//按第一高价计费(price1)
        	if("price1".equals(priceType)){
        		//第一高价为cpc
        		if("2".equals(bidType)){
        			revenue = bidPrice / 100.0;
        		}
          	}
        	//按第二高价计费(price2)
        	if("price2".equals(priceType)){
        		//两个价格计费方式相同，并且都是cpc
        		if("2".equals(bidType) && "2".equals(bidType2)){
        			revenue = bidPrice2 / 100.0;
        		}
        		//第一高价为cpc,第二高价为cpm
        		if("2".equals(bidType) && "1".equals(bidType2)){
        			revenue = (ecpm2 * bidPrice * 1.0) / (ecpm * 100.0);
        		}
        		//第一高价为cpm,第二高价为cpc
        		//两个价格都为cpm
        		//这两种情况都在展示日志里计算
        	}
		}
		return revenue;
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr
				+ "task"
				+ HDFSLogWritter.getFullNumber(context.getTaskAttemptID()
						.getTaskID().getId(), 4)
				+ "_"
				+ HDFSLogWritter.getFullNumber(
						(int) (System.currentTimeMillis() % 10000), 4), conf);
		LOG.info("AdPVClickDumpMapper setup!");
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		LOG.info("AdPVClickDumpMapper cleanup!");
		LOG.close();
	}

	@SuppressWarnings("unchecked")
	protected void map(BytesWritable key, Tuple value, Context context)
			throws IOException, InterruptedException {
		decode(key, value);

		String uid = (String) list.get(ConstData.CG_USER);
		if (uid == null) {
			LOG.error("PVClick uid is null");
			return;
		}
		uid = uid.trim();
		if (uid.equals("")) {
			LOG.error("PVClick uid is empty");
			return;
		}

		String bidType = null;
		String bidType2 = null;
		double bidPrice = 0.0;
		double bidPrice2 = 0.0;
		double ecpm = 0.0;
		double ecpm2 = 0.0;
		double revenue = 0.0;

		// 1,get pv data
		pvList = (List<AdInfoOperation>) list.get(ConstData.CG_ADDISPLAY);
		if (pvList != null && pvList.size() != 0) {
			StringBuffer tmp = null;
			for (AdInfoOperation pv : pvList) {
				// ad key
				String ad_key = pv.getMonitorKey();
				if (ad_key == null) {
					// LOG.error("PV ad_key is null");
					// continue;
					ad_key = "null";
				}
				ad_key = ad_key.trim();
				if (ad_key.equals("")) {
					// LOG.error("PV ad_key is empty");
					// continue;
					ad_key = "empty";
				}

				// region,time,type
				// String region = StringUtil.split(pv.getRegion());
				String region = pv.getRegion();
				long timeStamp = pv.getTimestamp();
				int statusCode = pv.getStatusCode();
				// String timeOfHour = sdf_hour.format(new Date(timeStamp));
				String timeOfHour = sdf_hour.format(new Date(timeStamp * 1000));
				String adType = pv.getAdType();
				// String position = pv.getAdPos();
				String adpid = pv.getAdpId();
				if (adpid != null) {
					String[] adtags = adpid.split("_");
					if (adtags.length == 2)
						adpid = adtags[1];
				}
				String position = adpid + "-" + pv.getTurn();

				if ("2".equalsIgnoreCase(adType))// filter non-ad type
					continue;
		
				bidType = pv.getBidType();
				bidType2 = pv.getBidType2();
				bidPrice = pv.getBidPrice();
				bidPrice2 = pv.getBidPrice2();
				ecpm = pv.getECPM();
				ecpm2 = pv.getECPM2();
				revenue = calculatePrice(bidType, bidType2, bidPrice,
						bidPrice2, ecpm, ecpm2, ConstData.PRICETYPE,
						ConstData.DT_ADDISPLAY);
					
				// build cube key
				tmp = new StringBuffer();
				tmp.append(ConstData.DT_ADDISPLAY);
				tmp.append("^^");
				tmp.append(ad_key);
				tmp.append("^^");
				tmp.append(statusCode);
				tmp.append("^^");
				tmp.append(region);
				tmp.append("^^");
				tmp.append(timeOfHour);
				tmp.append("^^");
				tmp.append(adType);
				tmp.append("^^");
				tmp.append(position);
				tmp.append("^^");
				tmp.append(String.valueOf(revenue));

				context.write(new Text(uid), new Text(tmp.toString()));
			}
		}

		// 2,get click data
		clickList = (List<AdInfoOperation>) list.get(ConstData.CG_ADCLICK);
		if (clickList != null && clickList.size() != 0) {
			StringBuffer tmp = null;
			for (AdInfoOperation pv : clickList) {
				// ad key
				String ad_key = pv.getMonitorKey();
				if (ad_key == null) {
					// LOG.error("PV ad_key is null");
					// continue;
					ad_key = "null";
				}
				ad_key = ad_key.trim();
				if (ad_key.equals("")) {
					// LOG.error("PV ad_key is empty");
					// continue;
					ad_key = "empty";
				}

				// region,time,type
				// String region = StringUtil.split(pv.getRegion());
				String region = pv.getRegion();
				long timeStamp = pv.getTimestamp();
				int statusCode = pv.getStatusCode();
				// String timeOfHour = sdf_hour.format(new Date(timeStamp));
				String timeOfHour = sdf_hour.format(new Date(timeStamp * 1000));
				String adType = pv.getAdType();
				// String position = pv.getAdPos();
				String adpid = pv.getAdpId();
				if (adpid != null) {
					String[] adtags = adpid.split("_");
					if (adtags.length == 2)
						adpid = adtags[1];
				}
				String position = adpid + "-" + pv.getTurn();

				if ("2".equalsIgnoreCase(adType))// filter non-ad type
					continue;

				bidType = pv.getBidType();
				bidType2 = pv.getBidType2();
				bidPrice = pv.getBidPrice();
				bidPrice2 = pv.getBidPrice2();
				ecpm = pv.getECPM();
				ecpm2 = pv.getECPM2();
				revenue = calculatePrice(bidType, bidType2, bidPrice,
						bidPrice2, ecpm, ecpm2, ConstData.PRICETYPE,
						ConstData.DT_ADCLICK);
				
				// build cube key
				tmp = new StringBuffer();
				tmp.append(ConstData.DT_ADCLICK);
				tmp.append("^^");
				tmp.append(ad_key);
				tmp.append("^^");
				tmp.append(statusCode);
				tmp.append("^^");
				tmp.append(region);
				tmp.append("^^");
				tmp.append(timeOfHour);
				tmp.append("^^");
				tmp.append(adType);
				tmp.append("^^");
				tmp.append(position);
				tmp.append("^^");
				tmp.append(String.valueOf(revenue));

				context.write(new Text(uid), new Text(tmp.toString()));
			}
		}
	}
}
