/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp
 * 文件名：FileNameLocInputFormat.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-上午9:57:43
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/**
 * 类名称：FileNameLocInputFormat
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月22日 上午9:57:43
 * 修改人：
 * 修改时间：2013年10月22日 上午9:57:43
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class FileNameLocInputFormat extends FileInputFormat<Text, Text> {
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		FileNameLocRecordReader reader = new FileNameLocRecordReader();
		try {
			reader.initialize(split, context);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return (RecordReader<Text, Text>) reader;
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return null;
	}

}
