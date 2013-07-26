package util;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class HDFSLogWritter {
	
	private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private String filename = "/user/aalog/pvinsight/logs/2012/10/31/log";
	private String uri = filename;
	private Configuration conf = null;
	private JobConf jconf = null;
	private HDFSFileWriter hfu;
	private int filesize = 0;
	private int part = 0;
	private static final int MAX_FILESIZE = 127 * 1024 * 1024;
	
//	public HDFSLogWritter() {
//		Configuration conf = new Configuration();
//		setup(conf);
//	}
//	public HDFSLogWritter(Configuration conf) {
//		setup(conf);
//	}
	public HDFSLogWritter(String uri, Configuration conf) {
		if(uri != null && uri.length() > 0) {
			this.uri = uri;
			filename = uri + "part_" + getPartStr(part);
		}
		setup(conf);
	}
	
	public HDFSLogWritter(String uri, JobConf conf) {
		if(uri != null && uri.length() > 0) {
			this.uri = uri;
			filename = uri + "part_" + getPartStr(part);
		}
		setup(conf);
	}
	
	public void info(String str) {
		String s = "[info] " + df.format(new Date()) + " | " + str + "\n";
		append(s);
		filesize += s.length();
		if(filesize > MAX_FILESIZE) {
			newpart();
		}
	}
	public void warn(String str) {
		String s = "[warn] " + df.format(new Date()) + " | " + str + "\n";
		append(s);
		filesize += s.length();
		if(filesize > MAX_FILESIZE) {
			newpart();
		}
	}
	public void error(String str) {
		String s = "[error] " + df.format(new Date()) + " | " + str + "\n";
		append(s);
		filesize += s.length();
		if(filesize > MAX_FILESIZE) {
			newpart();
		}
	}
	public void debug(String str) {
		String s = "[debug] " + df.format(new Date()) + " | " + str + "\n";
		append(s);
		filesize += s.length();
		if(filesize > MAX_FILESIZE) {
			newpart();
		}
	}
	public void none(String str) {
		append(str);
		filesize += str.length();
		if(filesize > MAX_FILESIZE) {
			newpart();
		}
	}
	
	private void setup(Configuration conf) {
		this.conf = conf;
		hfu= new HDFSFileWriter(filename, conf);
	}
	
	private void setup(JobConf conf) {
		this.jconf = conf;
		hfu= new HDFSFileWriter(filename, conf);
	}
	private void append(String str) {
		try {
			hfu.append(str);
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
//	public void flush() {
//		try {
//			hfu.flush();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	public void close() {
		try {
			hfu.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void newpart() {
		close();
		part ++;
		filename = this.uri +"part_" + getPartStr(part);
		filesize = 0;
		
		if(conf != null)
			setup(conf);
		else
			setup(jconf);
	}
	
	public String getPartStr(int part) {
		String s = Integer.toString(part);
		int length = s.length();
		for(int i = 0 ; i < 3 - length ; i ++) {
			s = "0" + s;
		}
		return s;
	}
	
	public static String getFullNumber(int number, int length) {
		String res = Integer.toString(number);
		int reslength = res.length();
		for(int i = 0 ; i < length - reslength ; i ++) {
			res = "0" + res;
		}
		return res;
	}
	
	public String getUri() {
		return filename;
	}
	
	public static void main(String[] args) {
		System.out.println(getFullNumber(1, 4));
	}
	
//	public static void main(String[] args) {
//		HDFSLogWritter LOG = new HDFSLogWritter();
//		LOG.info("info log");
//		LOG.warn("warn log");
//		LOG.error("error log");
//		LOG.debug("debug log");
//		LOG.close();
//	}
		
}
