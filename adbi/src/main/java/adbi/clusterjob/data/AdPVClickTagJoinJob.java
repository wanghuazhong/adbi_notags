package adbi.clusterjob.data;

import java.io.File;
import java.io.IOException;

import adbi.mapreduce.data.AdPVClickFormatMapper;
import adbi.mapreduce.data.AdPVClickTagJoinReducer;
import adbi.mapreduce.data.UserTagsFormatMapper;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.ConstData;
import util.StringUtil;

@SuppressWarnings("deprecation")
public class AdPVClickTagJoinJob extends Configured implements Tool {

	final String JOB_NAME = "ADBI_AdPVClickTagJoinJob";
	
	private JobConf config_;
	
	private Options allOptions;
	private String[] argv_;
	
	private String output_;
	private String tagInput_;
	private String logInput_;
	private String time_;
	private String lib_;
	private String numReduceTasks_;
	private int  reduceNum = 1;
	
	private FileSystem client = null;
	//private Path[] inputPaths = null;
	
	private static final Log LOG = LogFactory.getLog(AdPVClickTagJoinJob.class);
	
	public AdPVClickTagJoinJob() {
		setupOptions();
		this.config_ = new JobConf(this.getClass().getSimpleName());
	}
	
	public static void main(String[] args) throws Exception {
		int ret = 0;
		AdPVClickTagJoinJob job = new AdPVClickTagJoinJob();
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

		//set boot main class
		config_.setJarByClass(this.getClass());
		
		//basic IO
		//TextOutputFormat.setOutputPath(config_, new Path(output_));
		SequenceFileOutputFormat.setOutputPath(config_, new Path(output_));
		
		//config_.setOutputFormat(TextOutputFormat.class);
		config_.setOutputFormat(SequenceFileOutputFormat.class);
		
		//MapReduce class
		//job.setMapperClass(UserTagsDumpMapper.class);
		//config_.setPartitionerClass(HashPartitioner.class);
		config_.setNumReduceTasks(reduceNum);
		config_.setMapOutputKeyClass(Text.class);
		config_.setMapOutputValueClass(Text.class);
		config_.setOutputKeyClass(Text.class);
		config_.setOutputValueClass(Text.class);
        
        config_.setReducerClass(AdPVClickTagJoinReducer.class);
        MultipleInputs.addInputPath(config_, new Path(logInput_), SequenceFileInputFormat.class, AdPVClickFormatMapper.class);
        MultipleInputs.addInputPath(config_, new Path(tagInput_), SequenceFileInputFormat.class, UserTagsFormatMapper.class);
        
        //configJob(config_);
        try {
        	JobClient.runJob(config_);
		} catch (Exception e) {
			LOG.error("submit " + JOB_NAME + " error for : " + e);
			return 1;
		}
		return 0;
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
		config_.set("mapred.job.name", JOB_NAME);
		//config_.set("mapred.mapper.new-api", "true");
		//config_.set("mapred.reducer.new-api", "true");
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
		Option tagInput = createOption("tagInput", "DFS input file(s) for the Map step", "path", Integer.MAX_VALUE, true);
		Option logInput = createOption("logInput", "DFS input file(s) for the Map step", "path", Integer.MAX_VALUE, true);
		Option output = createOption("output", "DFS output directory for the Reduce step", "path", 1, true);
		Option libjars = createOption("lib", "sessionlog mapreduce lib jars", "file", Integer.MAX_VALUE, true);
		Option numReduceTasks = createOption("numReduceTasks", "Optional.", "spec", 1, true);
		
		allOptions = new Options().addOption(time).addOption(tagInput).addOption(logInput).addOption(output)
						.addOption(libjars).addOption(numReduceTasks);
	}
	
	public void exitUsage() {
		System.out.println("Usage: $HADOOP_HOME/bin/hadoop jar $SESSIIONLOG_HOME/sessionlog.jar [options]");
		System.out.println("Options:");
		System.out.println("  -time <yyyy/MM/dd> DFS input time for the Map step");
		System.out.println("  -tagInput <path> DFS input file(s) for the Map step");
		System.out.println("  -logInput <path> DFS input file(s) for the Map step");
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
			tagInput_ = (String) cmdLine.getOptionValue("tagInput");
			logInput_ = (String) cmdLine.getOptionValue("logInput");
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
				|| StringUtil.isBlank(tagInput_) 
				|| StringUtil.isBlank(logInput_)
				|| StringUtil.isBlank(output_)
				|| StringUtil.isBlank(numReduceTasks_)) {
			exitUsage();
			message = "time=[" + timeStr + "]\ninput1=[" 
							+ tagInput_ + "]\ninput2=[" 
							+ logInput_ + "]\noutput=[" 
							+ output_ + "]\nnumReduceTasks=[" 
							+ numReduceTasks_ + "]\n";
			fail(message);
		}
		
		//process input1
		if(client == null) {
			client = FileSystem.get(config_);
		}
		
		if(tagInput_.endsWith("/")) {
			tagInput_ = tagInput_.substring(0, tagInput_.length() - 1);
		}
		
		tagInput_ += ("/" + timeStr + "/step2");
		FileStatus fileStat = null;
		try {
			fileStat = client.getFileStatus(new Path(tagInput_));
		} catch (Exception e) {
			LOG.error("Get file status error:" + e.getMessage());
			e.printStackTrace();
		}
		
		if(fileStat == null) {
			message = "path : " + tagInput_ + " not found,redirect to empty step2[/user/aalog/ad_bi/empty_targeting_step2]";
			tagInput_ = "/user/aalog/ad_bi/empty_targeting_step2";
			//fail(message);
			LOG.warn(message);
		}
		LOG.info("tagInput:" + tagInput_);
		
		//process input2
		if(logInput_.endsWith("/")) {
			logInput_ = logInput_.substring(0, logInput_.length() - 1);
		}
		
		logInput_ += "/" + timeStr;
		fileStat = null;
		try {
			fileStat = client.getFileStatus(new Path(logInput_));
		} catch (Exception e) {
			LOG.error("Get file status error:" + e.getMessage());
			e.printStackTrace();
		}
		
		if(fileStat == null) {
			message = "path : " + logInput_ + " not found";
			fail(message);
		}
		LOG.info("logInput:" + logInput_);
		
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
	
	public JobConf getConf() {
		return config_;
	}

	public void setConf(JobConf conf) {
		config_ = conf;
	}

	@SuppressWarnings("static-access")
	private Option createOption(String name, String desc, String argName, int max, boolean required) {
		return OptionBuilder.withArgName(argName).hasArgs(max).withDescription(desc).isRequired(required).create(name);
	}

	private static void fail(String message) {
		System.err.println(message);
		throw new IllegalArgumentException(message);
	}

}

