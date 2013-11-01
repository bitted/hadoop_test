package com.lakala.itcast.combiner;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 类名称：WordCountApp
 * 类描述：(自定义字数统计)
 * 创建人：litj
 * 创建时间：2013年9月26日 下午4:45:05
 * 修改人：
 * 修改时间：2013年9月26日 下午4:45:05
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

	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		/**
		 * 1、加载默认的hadoop配置文件
		 */
		final Configuration conf = new Configuration();
		/**
		 * 2、删除输出目录
		 */
		deleteOutputDirectory(conf);
		/**
		 * 3、创建执行mapreduce的job任务
		 */
		final Job job = new Job(conf);
		/**
		 * TODO： 本地编译代码时，如果不加入这行，会报找不到类的异常
		 */
//		job.setJarByClass(WordCountApp.class);
		/**
		 * 4、设置输入目录，可能是一个文件
		 */
		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));
		/**
		 * 5、设置自定义的mapper实现类
		 */
		job.setMapperClass(MyMapper.class);
		/**
		 * 6、定义combiner实现类，这里使用和Reducer一样处理
		 * 这里处理分区、排序和分组的策略
		 */
		job.setCombinerClass(MyReducer.class);
		/**
		 * 7、定义reducer实现类
		 */
		job.setReducerClass(MyReducer.class);
		/**
		 * 8、定义输出的key的类型
		 */
		job.setOutputKeyClass(Text.class);
		/**
		 * 9、定义输出的value的类型
		 */
		job.setOutputValueClass(IntWritable.class);
		/**
		 * 10、设置输出目录
		 */
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));
		/**
		 * 11、true表示执行完成job后，输出完成的日志到控制台
		 */
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
//			System.out.println("===key:" + key.get() + ",==value:" + value.toString());
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
			System.out.println("========reduce==========");
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();
				System.out.println("===输出对==<key,value>===<" + key.toString() + "," + sum + ">");
			}
			context.write(key, new IntWritable(sum));
		}

	}

}
