package com.lakala.itcast.suanfa;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 问题1：倒序输出结果
 * 问题2：保持原有数据量的情况下排序？tip:二次排序
 */
public class SortApp {
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/SortAppinput";
	public static final String OUTPUT_URL = "/SortAppoutput";
	public static void main(String[] args) throws Exception{
		final Configuration conf = new Configuration();
		
		deleteOutputDirectory(conf);
		
		final Job job = new Job(conf);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL+INPUT_URL));
		
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL+OUTPUT_URL));
		
		job.waitForCompletion(true);
	}
	//删除输出目录
	private static void deleteOutputDirectory(final Configuration conf)
			throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable>{
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			System.out.println("=value=" +value);
			context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
		};
	}
	
	static class MyReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable>{
		protected void reduce(IntWritable k2, java.lang.Iterable<NullWritable> v2s, Context ctx) throws java.io.IOException ,InterruptedException {
			System.out.println("=k2=" +k2);
			ctx.write(k2, NullWritable.get());
		};
	}
}