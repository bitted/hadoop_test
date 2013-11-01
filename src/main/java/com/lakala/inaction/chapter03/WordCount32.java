package com.lakala.inaction.chapter03;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount32 extends Configured implements Tool {
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/inaction/32/input/input.txt";
	public static final String OUTPUT_URL = "/inaction/32/output";

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(WordCount32.class);
		job.setJobName("wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
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
	public static void deleteOutputDirectory(final Configuration conf, String OUTPUT_URL) throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);

		/**
		 * 删除一个目录，设置为true表示递归删除一个目录
		 */
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	public static void main(String[] args) throws Exception {
		deleteOutputDirectory(new Configuration(), OUTPUT_URL);
		int ret = ToolRunner.run(new WordCount32(), args);
		System.exit(ret);
	}
}