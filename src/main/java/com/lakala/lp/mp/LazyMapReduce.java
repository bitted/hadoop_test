package com.lakala.lp.mp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 类名称：LazyMapReduce
 * 类描述：(使用初始的Mapper和Reducer得到的结果。)
 * 创建人：litj
 * 创建时间：2013年10月15日 下午5:28:16
 * 修改人：
 * 修改时间：2013年10月15日 下午5:28:16
 * 修改备注：
 * 
 * @version 1.0.0
 */
public class LazyMapReduce {
	/**
	 * 定义hadoop的hdfs的url和输入输出目录
	 */
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/InvertedIndex/input/";
	public static final String OUTPUT_URL = "/InvertedIndex/output";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "LazyMapReduce");
		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
