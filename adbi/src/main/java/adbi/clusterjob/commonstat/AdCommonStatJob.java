package adbi.clusterjob.commonstat;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.ConstData;
import util.StringUtil;
import adbi.mapreduce.commonstat.AdCommonStatCombiner;
import adbi.mapreduce.commonstat.AdCommonStatMapper;
import adbi.mapreduce.commonstat.AdCommonStatReducer;

public class AdCommonStatJob implements Tool  {
	
	 String JOB_NAME = "ADBI_AdCommonStatJob_withOutTags_";
	
	private Configuration config_;
	
	private Options allOptions;
	private String[] argv_;
	
	private String output_;
	private String input_;
	private String time_;
	private String lib_;
	private String numReduceTasks_;
	private int  reduceNum = 1;
	
	private FileSystem client = null;
	private Path[] inputPaths = null;
	
	private static final Log LOG = LogFactory.getLog(AdCommonStatJob.class);
	
	public AdCommonStatJob() {
		setupOptions();
		this.config_ = new Configuration();
	}
	
	public static void main(String[] args) throws Exception {
		int ret = 0;
		AdCommonStatJob job = new AdCommonStatJob();
		ret = ToolRunner.run(job, args);
		if(ret != 0) {
			LOG.error("Streaming Job Failed!");
			System.exit(ret);
		}
	}

	public int run(String[] args) throws Exception {
		try {
			StringBuffer sb = new StringBuffer();
			for(String arg : args) {
				sb.append(arg).append(" ");
			}
			
			LOG.info("submit " + JOB_NAME + ":" + sb.toString());
			this.argv_ = args;
			parseArgv();
			postProcessArgs();
			setJobConf();
			return submitAndMonitorJob();
		} catch (IllegalArgumentException ex) {
			LOG.warn("submit " + JOB_NAME + ", error for : " + ex);
			return 1;
		}
	}
	
	private int submitAndMonitorJob() throws IOException {
		Job job = new Job(config_);
		//set boot main class
		job.setJarByClass(this.getClass());
		
		//basic IO
		//TextInputFormat.setInputPaths(job, inputPaths);
		SequenceFileInputFormat.setInputPaths(job, inputPaths);
		//TextOutputFormat.setOutputPath(job, new Path(output_));
		SequenceFileOutputFormat.setOutputPath(job, new Path(output_));
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//MapReduce class
		job.setMapperClass(AdCommonStatMapper.class);
		job.setCombinerClass(AdCommonStatCombiner.class);
		job.setReducerClass(AdCommonStatReducer.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(reduceNum);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        try {
			job.submit();
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		} catch (Exception e) {
			LOG.error("submit sessionlog job error for : " + e);
			return 1;
		}
	}
	
	private void setJobConf() throws IOException {
		final StringBuffer jarFileBuf = new StringBuffer();
		if(!lib_.endsWith("/")) {
			lib_ = lib_ + "/";
		}
		
		File libFile = new File(lib_);
		String libFiles[] = libFile.list();
		
		boolean first = true;
		
		for(String fileName : libFiles) {
			if(fileName.endsWith(".jar")) {
				if(first) { 
					first = false;
				} else {
					jarFileBuf.append(",");
				}
				
				jarFileBuf.append("file://").append(lib_).append(fileName);
			}
		}
		LOG.debug("JarFiles:" + jarFileBuf.toString());
		
		config_.set("tmpjars", jarFileBuf.toString());
		config_.set("mapred.job.name", JOB_NAME + time_);
		config_.set("mapred.mapper.new-api", "true");
		config_.set("mapred.reducer.new-api", "true");
		config_.set("mapred.output.dir", output_);
		config_.set("mapred.task.timeout", "1200000");// failed to report status
        
        Path outputPath = new Path(output_);
        FileSystem fs = FileSystem.get(outputPath.toUri(), config_);
        if(fs.exists(outputPath)) {
        	exitUsage();
        	fail("output path [" + output_ + "] path exist");
        }
	}
	
	private void setupOptions() {
		Option time = createOption("time", "DFS input time for the Map step", "yyyy/MM/dd", Integer.MAX_VALUE, true);
		Option input = createOption("input", "DFS input file(s) for the Map step", "path", Integer.MAX_VALUE, true);
		Option output = createOption("output", "DFS output directory for the Reduce step", "path", 1, true);
		Option libjars = createOption("lib", "sessionlog mapreduce lib jars", "file", Integer.MAX_VALUE, true);
		Option numReduceTasks = createOption("numReduceTasks", "Optional.", "spec", 1, true);
		
		allOptions = new Options().addOption(time).addOption(input).addOption(output)
						.addOption(libjars).addOption(numReduceTasks);
	}
	
	public void exitUsage() {
		System.out.println("Usage: $HADOOP_HOME/bin/hadoop jar $SESSIIONLOG_HOME/sessionlog.jar [options]");
		System.out.println("Options:");
		System.out.println("  -time <yyyy/MM/dd> DFS input time for the Map step");
		System.out.println("  -input <path> DFS input file(s) for the Map step");
		System.out.println("  -output <path> DFS output directory for the Reduce step");
		System.out.println("  -lib <path> jars adpvinsight job needs");
		System.out.println("  -numReduceTasks <int> adpvinsight job reduce number");
	}
	
	protected void parseArgv() {
		CommandLine cmdLine = null;
		
		try {
			cmdLine = new BasicParser().parse(allOptions, argv_);
		} catch (Exception e) {
			exitUsage();
			fail("parser cmd error:" + e.toString());
		}
		
		if(cmdLine != null) {
			time_ = (String) cmdLine.getOptionValue("time");
			input_ = (String) cmdLine.getOptionValue("input");
			output_ = (String) cmdLine.getOptionValue("output");
			lib_ = (String) cmdLine.getOptionValue("lib");
			numReduceTasks_ = (String) cmdLine.getOptionValue("numReduceTasks");
			
		} else {
			exitUsage();
			fail("parser cmd error, is null");
		}
	}
	
	private void postProcessArgs() throws IOException {
		String message = null;
		//param validation
		String timeStr = time_;
		if(StringUtil.isBlank(timeStr) 
				|| StringUtil.isBlank(input_) 
				|| StringUtil.isBlank(output_)
				|| StringUtil.isBlank(numReduceTasks_)) {
			exitUsage();
			message = "time=[" + timeStr + "]\ninput=[" 
							+ input_ + "]\noutput=[" 
							+ output_ + "]\nnumReduceTasks=[" 
							+ numReduceTasks_ + "]\n";
			fail(message);
		}
		
		//process input
		if(input_.endsWith("/")) {
			input_ = input_.substring(0, input_.length() - 1);
		}
		
		if(client == null) {
			client = FileSystem.get(config_);
		}
		
		input_ += "/" + timeStr;
		FileStatus fileStat = null;
		try {
			fileStat = client.getFileStatus(new Path(input_));
		} catch (Exception e) {
			LOG.error("Get file status error:" + e.getMessage());
			e.printStackTrace();
		}
		
		if(fileStat == null) {
			message = "path : " + input_ + " not found";
			fail(message);
		}
		
		if(fileStat.isDir()) {
			FileStatus subs[] = client.listStatus(new Path(input_));
			if(subs == null) {
				message = "input path : " + input_ + " not configed error";
				fail(message);
			}
				
			List<Path> pathList = new ArrayList<Path>();
			
			for(FileStatus sub : subs) {
				if(!sub.getPath().getName().contains("part")) {
					continue;
				}
				String subInput = input_ + "/" + sub.getPath().getName();
				pathList.add(new Path(subInput));
				LOG.info("subInput:" + subInput);
			}
			
			int i = 0;
			inputPaths = new Path[pathList.size()];
			
			for(Path p : pathList) {
				inputPaths[i] = p;
				i++;
			}
		}
		
		//process output
		if(output_.endsWith("/")) {
			output_ = output_ + timeStr;
		} else {
			output_ = output_ + "/" + timeStr;
		}
		
		fileStat = null;
		try {
			fileStat = client.getFileStatus(new Path(output_));
		} catch (Exception e) {
			
		}
		
		if(fileStat != null) {
			fail("output path:" + output_ + " exists");
		}
		
		try {
			reduceNum = Integer.parseInt(numReduceTasks_);
		} catch (NumberFormatException e) {
			fail("reduce task number not int");
		}
		
		if(reduceNum > ConstData.MAX_REDUCE) {
			fail("reduce task number [" + reduceNum + "] too large");
		}
		
		if(client != null) {
			client.close();
		}
	}
	
	public void fail(String message) {
		LOG.error(message);
		throw new IllegalArgumentException(message);
	}
	
	@SuppressWarnings("static-access")
	private Option createOption(String name, String desc, String argName, int max, boolean required) {
		return OptionBuilder.withArgName(argName).hasArgs(max).withDescription(desc).isRequired(required).create(name);
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.config_ = conf;
	}

	@Override
	public Configuration getConf() {
		return config_;
	}
}
