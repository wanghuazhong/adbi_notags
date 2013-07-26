package util;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSFileWriter {

	private BufferedWriter out = null;
	
	public HDFSFileWriter(String uri, Configuration conf) {
		FileSystem hdfsDes;
		try {
			hdfsDes = FileSystem.get(conf);
			Path desPath=new Path(uri);//目的路径
			FSDataOutputStream fout=hdfsDes.create(desPath);
			out=new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void append(String str) throws IOException {
		out.write(str);
	}
	
	public void flush() throws IOException {
		if(out != null) {
			out.flush();
		}
	}

	public void close() throws IOException {
		if(out != null){
			out.flush();
			out.close();
		}
	}


}