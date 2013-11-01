/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.counter
 * 文件名：WordCountApp.java
 * 版本信息：V1.0
 * 日期：2013年9月27日-上午9:38:36
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */
package com.lakala.itcast.counter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 类名称：WordCountApp
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年9月27日 上午9:38:36
 * 修改人：
 * 修改时间：2013年9月27日 上午9:38:36
 * 修改备注：
 * 
 * @version 1.0.0
 */
public class WordCountApp {
	/**
	 * 定义hadoop的hdfs的url和输入输出目录
	 */
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/input/test.txt";
	public static final String OUTPUT_URL = "/output";

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		deleteOutputDirectory(conf);

		final Job job = new Job(conf);
		job.setJarByClass(WordCountApp.class);

		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));

		job.setMapperClass(MyMapper.class);

		job.setCombinerClass(MyReducer.class);

		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		job.waitForCompletion(true);
	}

	/**
	 * deleteOutputDirectory(删除输出目录，因为hadoop的输出目录必须是不存在的才能够输出)
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

		/**
		 * 删除一个目录，设置为true表示递归删除一个目录
		 */
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	/**
	 * 类名称：MyMapper
	 * 类描述：(自定义mapper类)
	 * 创建人：litj
	 * 创建时间：2013年9月26日 下午5:27:56
	 * 修改人：
	 * 修改时间：2013年9月26日 下午5:27:56
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("========mappper==========");
			System.out.println("===key:" + key.get() + ",==value:" + value.toString());
			final String line = value.toString();
			final String[] strs = line.split(" ");
			for (String word : strs) {
				context.write(new Text(word), new IntWritable(1));
				System.out.println("===map后==<key,value>===<" + word + ",1>");
			}
		}
	}

	/**
	 * 类名称：MyReducer
	 * 类描述：(自定义reducer实现类)
	 * (主要就是key出现次数的累加）
	 * 创建人：litj
	 * 创建时间：2013年9月26日 下午5:48:32
	 * 修改人：
	 * 修改时间：2013年9月26日 下午5:48:32
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
				System.out.println("===输出对==<key,value>===<" + key.toString() + "," + sum + ">");
			}
			context.write(key, new IntWritable(sum));
		}

	}
}