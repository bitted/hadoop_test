/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp.job
 * 文件名：ChainMapReduce.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-下午2:34:26
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * 类名称：ChainMapReduce
 * 类描述：(MapReduce前处理和后处理步骤的链式执行)
 * 创建人：litj
 * 创建时间：2013年10月22日 下午2:34:26
 * 修改人：
 * 修改时间：2013年10月22日 下午2:34:26
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class ChainMapReduce {

	public static void main(String[] args) throws Exception {
		// 初始化作业
		Configuration conf = new Configuration();
/**
 * 
 	Job job = new Job(conf);
		job.setJobName("ChainJob");
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 在ChainMapper中加入Map1和Map2
		Configuration map1Conf = new Configuration();
		ChainMapper.addMapper(job, Map1.class, LongWritable.class, Text.class, Text.class, Text.class, true, map1Conf);

		Configuration map2Conf = new Configuration();
		ChainMapper.addMapper(job, Map2.class, LongWritable.class, Text.class, Text.class, Text.class, true, map2Conf);

		// 在ChainReducer中加入Reducer，map3和map4
		Configuration reduceConf = new Configuration(false);
		ChainMapper.addMapper(job, Reducer.class, LongWritable.class, Text.class, Text.class, Text.class, true, reduceConf);

		Configuration map3Conf = new Configuration();
		ChainMapper.addMapper(job, Map3.class, LongWritable.class, Text.class, Text.class, Text.class, true, map3Conf);

		Configuration map4Conf = new Configuration();
		ChainMapper.addMapper(job, Map4.class, LongWritable.class, Text.class, Text.class, Text.class, true, map4Conf);

		job.waitForCompletion(true);
*/
	
	}

}
