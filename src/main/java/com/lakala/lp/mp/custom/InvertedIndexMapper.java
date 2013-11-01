/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp
 * 文件名：InvertedIndexMapper.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-上午9:41:07
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 类名称：InvertedIndexMapper
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月22日 上午9:41:07
 * 修改人：
 * 修改时间：2013年10月22日 上午9:41:07
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// 使用缺省的TextInputFormat、LineRecordReader
		// 主键key：line offset； value：line string
		Text word = new Text();
		// 读取所需的FileName
		FileSplit fileSplit = (FileSplit) context.getInputSplit();

		String fileName = fileSplit.getPath().getName();

		Text fileName_LineOffset = new Text(fileName + "@" + key.toString());
		StringTokenizer token = new StringTokenizer(value.toString());
		while (token.hasMoreTokens()) {
			word.set(token.nextToken());
			context.write(word, fileName_LineOffset);
		}
	}
}
