/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp
 * 文件名：FileNameRecordReader.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-上午10:03:00
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 类名称：FileNameRecordReader
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月22日 上午10:03:00
 * 修改人：
 * 修改时间：2013年10月22日 上午10:03:00
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class FileNameLocRecordReader extends RecordReader<Text, Text> {
	String fileName;
	FileNameLocRecordReader reader = new FileNameLocRecordReader();

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
		fileName = ((FileSplit) split).getPath().getName();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text("(" + fileName + "@" + reader.getCurrentKey() + ")");
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {

	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit,
	 * org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */

	@Override
	public void initialize(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub

	}

}
