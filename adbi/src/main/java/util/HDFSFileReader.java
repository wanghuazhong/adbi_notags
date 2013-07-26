package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * <PRE>
 * 浣滅敤 : 
 *     hdfs璇诲彇鏂囦欢
 * 鎺ュ彛 : 
 *     
 * 娉ㄦ剰 :
 *     
 * 鍘嗗彶 :
 * --------------------------------------------------------------------------------------------
 *        VERSION          DATE                BY          CHANGE/COMMENT
 * --------------------------------------------------------------------------------------------
 *             1.0            2012-11-19        Liuyu                    create
 * --------------------------------------------------------------------------------------------
 * </PRE>
 */
public class HDFSFileReader {
	
	/**
	 * 璇绘枃浠�
	 * @param context
	 * @param configfile
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public String read(Context context, String filename) throws IOException {
		FileSystem fs;
		FSDataInputStream fin;
		BufferedReader in = null;
		fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(filename);
		StringBuffer buffer = new StringBuffer();
		if (fs.isFile(path)) {
			fin = fs.open(path);
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				String line = null;
				while ((line = in.readLine()) != null) {
					buffer.append(line).append("\n");
				}
			} catch (IOException ex) {
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}
		return buffer.toString();
	}
}
