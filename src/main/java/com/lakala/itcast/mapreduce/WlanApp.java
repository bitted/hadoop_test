/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.mapreduce
 * 文件名：WlanApp.java
 * 版本信息：V1.0
 * 日期：2013年10月8日-上午10:40:39
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.itcast.mapreduce;

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

/**
 * 类名称：WlanApp
 * 类描述：(拼接字符串实现的,使用系统提供的输出格式)
 * 	(这种方式适合于普通业务模式，如按照tab分割和空格分割，如果需要复杂的输出处理的类型时，就需要自定义value输出类，显示Writable接口)
 * @see WlanAppMe
 * 创建人：litj
 * 创建时间：2013年10月8日 上午10:40:39
 * 修改人：
 * 修改时间：2013年10月8日 上午10:40:39
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class WlanApp {

	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/wlaninput";
	public static final String OUTPUT_URL = "/wlanoutput";

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

	/**
	 * deleteOutputDirectory(删除输出目录)
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws URISyntaxException
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	private static void deleteOutputDirectory(final Configuration conf) throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			// 手机号k2
			Text k2 = new Text(splited[1]);
			// 表示每行手机号的流量情况
			Text v2 = new Text(splited[6] + "\t" + splited[7] + "\t" + splited[8] + "\t" + splited[9]);

			context.write(k2, v2);

		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		// k2是手机号
		// v2s里面装的是k2的多个行记录（record）的流量情况
		protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
			int upPackNum = 0;
			int downPackNum = 0;
			int upPayLoad = 0;
			int downPayLoad = 0;

			for (Text v2 : v2s) {
				final String[] splited = v2.toString().split("\t");
				upPackNum += Integer.parseInt(splited[0]);
				downPackNum += Integer.parseInt(splited[1]);
				upPayLoad += Integer.parseInt(splited[2]);
				downPayLoad += Integer.parseInt(splited[3]);
			}

			context.write(k2, new Text(upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad));
		}

	}
}
