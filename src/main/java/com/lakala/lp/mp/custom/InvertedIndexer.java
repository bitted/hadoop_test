/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp.custom
 * 文件名：InvertedIndexer.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-上午10:32:31
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lakala.lp.mp.InvertedIndex.InvertedIndexReducer;

/**
 * 类名称：InvertedIndexer
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月22日 上午10:32:31
 * 修改人：
 * 修改时间：2013年10月22日 上午10:32:31
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class InvertedIndexer {
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "invert index");

			job.setJarByClass(InvertedIndexer.class);
//			job.setInputFormatClass(FileNameLocInputFormat.class);//报错
			job.setMapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
