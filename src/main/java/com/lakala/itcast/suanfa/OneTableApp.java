package com.lakala.itcast.suanfa;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
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

/**
 * 测试数据
 * feng li
 * wu feng
 * 
 * 解决方案：正序输出的时候，v2加标记
 */
public class OneTableApp {
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/onetableinput";
	public static final String OUTPUT_URL = "/onetableoutput";

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
			final String[] splited = value.toString().split("\t");
			System.out.println("==splited[0]=" + splited[0] + ",====splited[1]=" + splited[1]);
			context.write(new Text(splited[0]), new Text("#" + splited[1]));
			context.write(new Text(splited[1]), new Text(splited[0]));
		};
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text k2, java.lang.Iterable<Text> v2s, Context ctx) throws java.io.IOException,
				InterruptedException {
			Text id = new Text();
			Text ppid = new Text();

			for (Text v2 : v2s) {
				final String name = v2.toString();
				// 表示ppid
				if (name.startsWith("#")) {
					ppid.set(name.substring(1));
				}
				// 表示id
				else {
					id.set(name);
				}
				System.out.println("====name=" + name);
			}
			System.out.println("====id=" + id +",===ppid=" + ppid);
			if (StringUtils.isNotEmpty(id.toString()) && StringUtils.isNotEmpty(ppid.toString())) {
				ctx.write(id, ppid);
			}
		};
	}
}