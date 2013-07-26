package adbi.mapreduce.dataformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import util.ConstData;

@SuppressWarnings("deprecation")
public class PartitionByStatDimMTOF extends MultipleOutputFormat<Text, Text> {
	private TextOutputFormat<Text, Text> output = null;
	
	@Override
	protected RecordWriter<Text, Text> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable arg3) throws IOException {
		if (output == null) {
	          output = new TextOutputFormat<Text, Text>();
	        }
		return output.getRecordWriter(fs, job, name, arg3);
	}
	
	protected String generateFileNameForKeyValue(Text key, Text value, String filename)
	{
		//key fromat (statindex,statdim,indexvalue,dimvalue[option],adpos[option],adtype)
		String[] tags = key.toString().split("\\^\\^");
		if(tags.length < 4)
			return "ERROR_KEY/" + filename;
		
		String statindex = tags[0];
		String statdim = tags[1];
		
		String indexDir = ConstData.ID_DIR_MAP.get(statindex);
		String dimDir = ConstData.DM_DIR_MAP.get(statdim);
		
		if(indexDir == null)
			return "INDEX_NO_MAPPING/" + filename;
		
		if(dimDir == null)
			return "DIR_NO_MAPPING/" + filename;
		
		return indexDir + "/" + dimDir + "/" + filename;
	}

}
