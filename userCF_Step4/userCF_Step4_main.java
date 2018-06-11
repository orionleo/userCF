package userCF_Step4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import userCF_Tools.*;

public class userCF_Step4_main {

	private static String InputPath = "/userCF/Step2_Output/part-r-00000";
	private static String OutputPath = "/userCF/Step4_Output/";
	private static String HDFS = "hdfs://quickstart.cloudera:8020";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFS);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(userCF_Step4_main.class);
		job.setMapperClass(userCF_Step4_Mapper.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setSortComparatorClass(mySortComparator.class);
		job.setGroupingComparatorClass(myGroupingComparator.class);
		job.setReducerClass(myReducer.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(InputPath);
		if(fs.exists(inputPath)) {
			FileInputFormat.addInputPath(job, inputPath);
		}
		Path outputPath = new Path(OutputPath);
		fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
