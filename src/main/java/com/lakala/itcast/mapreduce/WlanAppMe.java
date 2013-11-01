package com.lakala.itcast.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 使用自定义Writable实现
 */
public class WlanAppMe {
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
		job.setMapOutputValueClass(KpiWritable.class);

		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		job.waitForCompletion(true);
	}

	// 删除输出目录
	private static void deleteOutputDirectory(final Configuration conf)
			throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable> {
		// map处理每一行的日志内容
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			// 手机号是k2
			Text k2 = new Text(splited[1]);
			// 表示每行手机号的流量情况
			KpiWritable v2 = new KpiWritable(splited[6], splited[7],
					splited[8], splited[9]);
			context.write(k2, v2);
		};
	}

	static class MyReducer extends
			Reducer<Text, KpiWritable, Text, KpiWritable> {
		// k2是手机号
		// v2s里面装的是k2的多个行记录（record）的流量情况
		protected void reduce(
				Text k2,
				java.lang.Iterable<KpiWritable> v2s,
				org.apache.hadoop.mapreduce.Reducer<Text, KpiWritable, Text, KpiWritable>.Context ctx)
				throws java.io.IOException, InterruptedException {
			int upPackNum = 0;
			int downPackNum = 0;
			int upPayLoad = 0;
			int downPayLoad = 0;

			for (KpiWritable v2 : v2s) {
				upPackNum += v2.upPackNum;
				downPackNum += v2.downPackNum;
				upPayLoad += v2.upPayLoad;
				downPayLoad += v2.downPayLoad;
			}

			ctx.write(k2, new KpiWritable(upPackNum + "", downPackNum + "",
					upPayLoad + "", downPayLoad + ""));
		};
	}

}

class KpiWritable implements Writable {
	public int upPackNum = 0;
	public int downPackNum = 0;
	public int upPayLoad = 0;
	public int downPayLoad = 0;

	public KpiWritable() {
	}

	public KpiWritable(String upPackNum, String downPackNum, String upPayLoad,
			String downPayLoad) {
		this.upPackNum = Integer.parseInt(upPackNum);
		this.downPackNum = Integer.parseInt(downPackNum);
		this.upPayLoad = Integer.parseInt(upPayLoad);
		this.downPayLoad = Integer.parseInt(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readInt();
		this.downPackNum = in.readInt();
		this.upPayLoad = in.readInt();
		this.downPayLoad = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upPackNum);
		out.writeInt(downPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPayLoad);
	}

	@Override
	public String toString() {
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
	}

}
