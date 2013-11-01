/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.lp.mp
 * 文件名：InvertedIndex.java
 * 版本信息：V1.0
 * 日期：2013年10月15日-下午2:19:08
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 类名称：InvertedIndex
 * 类描述：(倒排索引实现)
 * 《Map(mapper->combiner)->Reducer(reducer)》
 * 创建人：litj
 * 创建时间：2013年10月15日 下午2:19:08
 * 修改人：
 * 修改时间：2013年10月15日 下午2:19:08
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class InvertedIndex {
	/**
	 * 定义hadoop的hdfs的url和输入输出目录
	 */
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";
	public static final String INPUT_URL = "/InvertedIndex/input/2.txt";
	public static final String OUTPUT_URL = "/InvertedIndex/output";

	/**
	 * 类名称：InvertedIndexMapper
	 * 类描述：(这里的将单词和URL组成key值（如：“MapReduce：1.txt”），将分词作为value，
	 * 这样的好处是可以利用MapReduce框架自带的Map端排序，将同一文档的单词的词频组成列表，传递给Combine过程，
	 * 实现类似于WordCount的功能。)
	 * <
	 * ========InvertedIndexMapper======map=========
	 * ===keyInfo=MapReduce:hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt==valueInfo=1
	 * ===keyInfo=is:hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt==valueInfo=1
	 * ===keyInfo=powerful:hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt==valueInfo=1
	 * ===keyInfo=is:hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt==valueInfo=1
	 * ===keyInfo=simple:hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt==valueInfo=1
	 * >
	 * 创建人：litj
	 * 创建时间：2013年10月15日 下午2:21:33
	 * 修改人：
	 * 修改时间：2013年10月15日 下午2:21:33
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private Text keyInfo = new Text();// 存储单子和URI的组合
		private Text valueInfo = new Text();// 存储词频
		private FileSplit split;// 存储Split对象

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("========InvertedIndexMapper======map=========");

			// 获得<key,value>对所属的FileSplit对象
			split = (FileSplit) context.getInputSplit();

			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				// key值由单词和URI组成，如“MapReduce：1.txt”
				keyInfo.set(itr.nextToken() + ":" + split.getPath().toString());
				// 词频初始为1
				valueInfo.set("1");

				System.out.println("===keyInfo=" + keyInfo + "==valueInfo=" + valueInfo);
				context.write(keyInfo, valueInfo);
			}
		}
	}

	/**
	 * 类名称：InvertedIndexCombiner
	 * 类描述：(经过map方法处理后，Combiner过程将key相同的value值累加，得到一个单词在文档中的词频，如图3-13所示。
	 * 如果直接将图3-13所示的输出作为Reduce过程的输入，在Shuffle过程时将面临一个问题：所有具有相同单词的记录（有
	 * 单词、URI和词频组成）应该交由同一个Reducer处理，但当前的key值无法保证这一点，所以必须修改key值和value值。
	 * 这次将单词作为key值，URI和词频组成value值，如1.txt：1。这样做的好处是可以利用MapReduce框架默认的HashPartitioner
	 * 类完成Shuffle过程，将相同单词的所有记录发送给同一个Reducer处理。)
	 * <
	 * ========InvertedIndexCombiner======reduce=========
	 * ===key=MapReduce==info=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1
	 * ========InvertedIndexCombiner======reduce=========
	 * ===key=is==info=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:2
	 * ========InvertedIndexCombiner======reduce=========
	 * ===key=powerful==info=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1
	 * ========InvertedIndexCombiner======reduce=========
	 * ===key=simple==info=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1
	 * >
	 * 创建人：litj
	 * 创建时间：2013年10月15日 下午2:49:12
	 * 修改人：
	 * 修改时间：2013年10月15日 下午2:49:12
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		private Text info = new Text();// 组合后输出到Reduce的value

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("========InvertedIndexCombiner======reduce=========");
			int sum = 0;// 统计词频

			for (Text value : values) {
				sum += Integer.parseInt(value.toString());
			}

			int splitIndex = key.toString().indexOf(":");

			// 重新设置value值与URI和词频组成
			info.set(key.toString().substring(splitIndex + 1) + ":" + sum);

			// 重新设置key值为单词
			key.set(key.toString().substring(0, splitIndex));
			System.out.println("===key=" + key + "==info=" + info);

			context.write(key, info);
		}
	}

	/**
	 * 类名称：InvertedIndexReducer
	 * 类描述：(经过上诉两个过程后，Reduce过程只需要将相同key值的value值组合成倒排索引文件所需要的格式即可，
	 * 剩下的事情就可以直接交给MapReduce框架处理了。)
	 * <
	 * ========InvertedIndexReducer======reduce=========
	 * ===key=MapReduce==result=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1;
	 * ========InvertedIndexReducer======reduce=========
	 * ===key=is==result=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:2;
	 * ========InvertedIndexReducer======reduce=========
	 * ===key=powerful==result=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1;
	 * ========InvertedIndexReducer======reduce=========
	 * ===key=simple==result=hdfs://10.5.12.66:9000/InvertedIndex/input/2.txt:1;
	 * >
	 * 创建人：litj
	 * 创建时间：2013年10月15日 下午5:11:25
	 * 修改人：
	 * 修改时间：2013年10月15日 下午5:11:25
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();// 最终输出的value

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("========InvertedIndexReducer======reduce=========");
			// 生成文档列表
			String fileList = new String();
			for (Text value : values) {
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			System.out.println("===key=" + key + "==result=" + result);

			context.write(key, result);
		}
	}

	/**
	 * main(倒排索引实例)
	 * 
	 * @param args
	 *            返回类型：void
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		deleteOutputDirectory(conf);

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		/**
		 * 如果这里不设置mapper，就会出现类型不匹配的错误，如
		 * java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.Text, recieved
		 * org.apache.hadoop.io.LongWritable
		 */
		job.setMapperClass(InvertedIndexMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(HADOOP_URL + INPUT_URL));
		FileOutputFormat.setOutputPath(job, new Path(HADOOP_URL + OUTPUT_URL));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
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
}
