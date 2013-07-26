package adbi.mapreduce.commonstat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.ConstData;
import util.HDFSLogWritter;

public class AdCommonStatMapper extends Mapper<Text, Text, Text, Text> {

	private final String LogAddr = "/user/aalog/ad_bi/log_tmp/AdCommonStatMapper/";
	private static HDFSLogWritter LOG = null;

	private static LongWritable ONE = new LongWritable(1);

	private Hashtable<String, String> userTagsLevel1 = new Hashtable<String, String>();
	private Hashtable<String, String> userTagsAll = new Hashtable<String, String>();
	private Hashtable<String, List<Set<String>>> m_targeting_definitions = new Hashtable<String, List<Set<String>>>();

	public static List<Set<String>> parse_definition(String line) {
		List<Set<String>> def = new ArrayList<Set<String>>();
		String[] ors = line.split("\\|");
		for (int i = 0; i < ors.length; ++i) {
			String and = ors[i].trim();
			if (!(and.startsWith("(") && and.endsWith(")"))) {
				LOG.error("targeting definition format error:" + line);
				return null;
			}

			and = and.substring(1, and.length() - 1).trim();
			if (and.equals(""))
				continue;

			String[] terms = and.split("\\&");
			Set<String> set = new HashSet<String>();
			for (int j = 0; j < terms.length; ++j)
				set.add(terms[j]);
			def.add(set);
		}
		return def;
	}

	public static String format_definition(List<Set<String>> orList) {
		if (orList == null)
			return null;

		boolean bfirst = true;
		StringBuilder tmp = new StringBuilder();
		for (int i = 0; i < orList.size(); ++i) {
			StringBuilder tt = new StringBuilder();
			tt.append("(");

			boolean bbfirst = true;
			Set<String> and = orList.get(i);
			Iterator<String> iter = and.iterator();
			while (iter.hasNext()) {
				String term = iter.next();
				if (!bbfirst)
					tt.append("&");
				bbfirst = false;
				tt.append(term);
			}

			tt.append(")");

			if (!bfirst)
				tmp.append("|");
			bfirst = false;
			tmp.append(tt);
		}
		return tmp.toString();
	}

	public static boolean judge_condition(Set<String> ands, String tag) {
		if (ands.contains("*"))
			return true;

		if (ands.contains("^"))
			return false;

		Iterator<String> iter = ands.iterator();
		while (iter.hasNext()) {
			String term = iter.next();
			if (!tag.contains(term))
				return false;
		}
		return true;
	}

	public List<String> map_tag_to_target(String tag) {
		if (tag == null)
			return null;

		try {
			List<String> res = new ArrayList<String>();
			Set<String> keys = m_targeting_definitions.keySet();
			Iterator<String> iter = keys.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				List<Set<String>> orList = m_targeting_definitions.get(key);

				for (int i = 0; i < orList.size(); ++i) {
					if (judge_condition(orList.get(i), tag)) {
						res.add(key);
						break;
					}
				}
			}
			return res;
		} catch (Exception e) {
			LOG.error("map error:" + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		LOG = new HDFSLogWritter(LogAddr
				+ "task"
				+ HDFSLogWritter.getFullNumber(context.getTaskAttemptID()
						.getTaskID().getId(), 4)
				+ "_"
				+ HDFSLogWritter.getFullNumber(
						(int) (System.currentTimeMillis() % 10000), 4), conf);
		// try
		// {
		// Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		// if(cacheFiles != null && cacheFiles.length > 0)
		// {
		// String line;
		// String[] tokens;
		//
		// //level-1 tags
		// BufferedReader joinReader = new BufferedReader(new
		// FileReader("user_tags_level1.txt"));
		// try{
		// while((line = joinReader.readLine()) != null){
		// tokens = line.split("\\t");
		// if(tokens.length != 2)
		// {
		// LOG.error("usertags fields not equals 2:" + line);
		// continue;
		// }
		// userTagsLevel1.put(tokens[0], tokens[1]);
		// //LOG.info("usertags level1:" + tokens[0] + "<=>" + tokens[1]);
		// }
		// }finally{
		// joinReader.close();
		// }
		//
		// //all tags
		// joinReader = new BufferedReader(new FileReader("user_tags_all.txt"));
		// try{
		// while((line = joinReader.readLine()) != null){
		// tokens = line.split("\\t");
		// if(tokens.length != 2)
		// {
		// LOG.error("usertags fields not equals 2:" + line);
		// continue;
		// }
		//
		// userTagsAll.put(tokens[0], tokens[1]);
		// //LOG.info("usertags all:" + tokens[0] + "<=>" + tokens[1]);
		// }
		// }finally{
		// joinReader.close();
		// }
		//
		// //parse targeting def file
		// joinReader = new BufferedReader(new FileReader("targeting_def.txt"));
		// try{
		// while((line = joinReader.readLine()) != null){
		// line = line.trim();
		// if(line.startsWith("#"))
		// continue;
		//
		// tokens = line.split("\\t");
		// if(tokens.length != 2)
		// {
		// LOG.error("targeting definition fields not equals 2:" + line);
		// continue;
		// }
		//
		// LOG.info("target:" + tokens[0] + "<=>" + tokens[1]);
		//
		// if(m_targeting_definitions.contains(tokens[0]))
		// LOG.warn("target already exist:" + tokens[0] +
		// ", definition override.");
		//
		// List<Set<String>> def = parse_definition(tokens[1]);
		// if(def == null || def.isEmpty())
		// {
		// LOG.warn("definition null, skip target def:" + line);
		// continue;
		// }
		//
		// m_targeting_definitions.put(tokens[0], def);
		// //LOG.info("put targeting def:" + tokens[0] + "<=>" +
		// format_definition(def));
		// }
		// }finally{
		// joinReader.close();
		// }
		// }
		// }catch (IOException e){
		// LOG.info("Exception reading distributedcache:" + e);
		// }
		//
		LOG.info("AdCommonStatMapper setup!");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		LOG.info("AdCommonStatMapper cleanup!");
		LOG.close();
	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String uid = key.toString();
		if (uid == null)
			return;
		uid = uid.trim();
		if (uid.equals(""))
			return;

		String adinfo = value.toString();
		if (adinfo == null)
			return;
		adinfo = adinfo.trim();
		if (adinfo.equals(""))
			return;

		String[] cols = adinfo.split("\\^\\^");
		if (cols.length < 6) {
			LOG.warn("Input fileds format error, value is less than 6[datatype,adkey,region,time,adtype,adpos,{,tags}] fileds. value:"
					+ adinfo + " fields count:" + cols.length);
			return;
		}

		String datatype = cols[0];
		String adkey = cols[1];
		String statusCode = cols[2];
		String region = cols[3];
		String time = cols[4];
		String adtype = cols[5];
		String adpos = cols[6];
		String revenue = cols[7];
//		String tagList = "";
//		if (cols.length == 7)
//			tagList = cols[6];

		// ////////////////////////////////////////visting
		// stat////////////////////////////////////////
		// uv
		if (datatype.equalsIgnoreCase(ConstData.DT_ADDISPLAY))
			context.write(new Text(datatype + "^^" + ConstData.ID_VISIT + "^^"
					+ ConstData.DM_USER + "^^" + adkey + "^^" + statusCode + "^^" + uid + "^^"
					+ adtype), new Text(revenue + "^^" + 1));

		// pv click on time dim
		context.write(new Text(datatype + "^^" + ConstData.ID_VISIT + "^^"
				+ ConstData.DM_TIME + "^^" + adkey + "^^" + statusCode + "^^" + time + "^^" + adpos
				+ "^^" + adtype), new Text(revenue + "^^" + 1));
		// pv click on region dim
		context.write(new Text(datatype + "^^" + ConstData.ID_VISIT + "^^"
				+ ConstData.DM_REGION + "^^" + adkey + "^^" + statusCode + "^^" + region + "^^" + adtype), new Text(revenue + "^^" + 1));
		// pv click on interest dim
		// boolean notags = true;
		// String[] tags = tagList.split(",");
		// for(int i = 0;i < tags.length;++i)
		// {
		// if(!tags[i].equals("") && userTagsLevel1.containsKey(tags[i]))
		// {
		// context.write(new Text(datatype + "^^" + ConstData.ID_VISIT + "^^" +
		// ConstData.DM_INTEREST + "^^" + adkey + "^^" + tags[i] + "^^" + adpos
		// + "^^" + adtype), ONE);
		// notags = false;
		// }
		// }
		// if(notags)//no tags pv click stat necessary?
		// context.write(new Text(datatype + "^^" + ConstData.ID_VISIT + "^^" +
		// ConstData.DM_INTEREST + "^^" + adkey + "^^" + "-1" + "^^" + adpos +
		// "^^" + adtype), ONE);

		// ////////////////////////////////////////reach
		// stat////////////////////////////////////////
		// Set<String> targets = new HashSet<String>();
		// for(int i = 0;i < tags.length;++i)
		// {
		// if(tags[i].equals("") || !userTagsAll.containsKey(tags[i]))
		// continue;
		//
		// List<String> lst = map_tag_to_target(userTagsAll.get(tags[i]));
		// if(lst == null || lst.isEmpty())
		// continue;
		//
		// for(int j = 0;j < lst.size();++j)
		// targets.add(lst.get(i));
		//
		// }
		//
		// for(java.util.Iterator<String> siter =
		// targets.iterator();siter.hasNext();)
		// {
		// String target = siter.next();
		// List<Set<String>> orList = m_targeting_definitions.get(target);
		// String targetDef = target + "=" + format_definition(orList);
		//
		// //uv
		// if(datatype.equalsIgnoreCase(ConstData.DT_ADDISPLAY))
		// context.write(new Text(datatype + "^^" + ConstData.ID_REACH + "^^" +
		// ConstData.DM_USER + "^^" + targetDef + "^^" + uid + "^^" + adtype),
		// ONE);
		//
		// //pv click on time dim
		// context.write(new Text(datatype + "^^" + ConstData.ID_REACH + "^^" +
		// ConstData.DM_TIME + "^^" + targetDef + "^^" + time + "^^" + adtype),
		// ONE);
		// //pv click on region dim
		// context.write(new Text(datatype + "^^" + ConstData.ID_REACH + "^^" +
		// ConstData.DM_REGION + "^^" + targetDef + "^^" + region + "^^" +
		// adtype), ONE);
		// }
	}

}
