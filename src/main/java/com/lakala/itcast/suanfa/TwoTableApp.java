package com.lakala.itcast.suanfa;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwoTableApp {
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/TwoTableAppinput";
	public static final String OUTPUT_URL = "/TwoTableAppoutput";

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();

		deleteOutputDirectory(conf);

		final Job job = new Job(conf);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));

		job.setMapperClass(MyMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		job.waitForCompletion(true);
	}

	// 删除输出目录
	private static void deleteOutputDirectory(final Configuration conf) throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			Text k2 = new Text();
			Text v2 = new Text();

			final String[] splited = value.toString().split("\t");
			// 读取的是用户表
			if (splited.length == 2) {
				k2.set(splited[0]);
				v2.set(splited[1]);
			} else
			// 读取的是角色表
			if (splited.length == 3) {
				k2.set(splited[2]);
				v2.set("#" + splited[1]);
			}
			System.out.println("===k2=" + k2 + ",===v2=" + v2);
			context.write(k2, v2);
		};
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text k2, java.lang.Iterable<Text> v2s, Context ctx) throws java.io.IOException,
				InterruptedException {
			Text role = new Text();
			Text user = new Text();

			for (Text v2 : v2s) {
				final String name = v2.toString();
				System.out.println("===name=" + name);
				// 表示角色
				if (name.startsWith("#")) {
					role.set(name.substring(1));
				}
				// 表示用户
				else {
					user.set(name);
				}
			}
			ctx.write(role, user);
		};
	}
}