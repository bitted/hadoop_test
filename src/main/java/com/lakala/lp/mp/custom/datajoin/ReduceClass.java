///**
// * 项目名称：拉卡拉手机钱包
// * 包名：com.lakala.lp.mp.custom.datajoin
// * 文件名：ReduceClass.java
// * 版本信息：V1.0
// * 日期：2013年10月23日-下午2:47:14
// * 作者：litj
// * Copyright (c) 2013拉卡拉支付有限公司-版权所有
// */
//
//package com.lakala.lp.mp.custom.datajoin;
//
//import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
//import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
//import org.apache.hadoop.io.Text;
//
///**
// * 类名称：ReduceClass
// * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
// * (这里描述此类业务作用，描述性说明 ----必填）
// * 创建人：litj
// * 创建时间：2013年10月23日 下午2:47:14
// * 修改人：
// * 修改时间：2013年10月23日 下午2:47:14
// * 修改备注：
// * 
// * @version 1.0.0
// * @param <TaggedRecordWritable>
// * @param <TaggedRecordWritable>
// */
//
//public class ReduceClass<TaggedRecordWritable> extends DataJoinReducerBase {
//
//	@Override
//	protected TaggedMapOutput combine(Object[] tags, Object[] values) {
//
//		// 一个以下数据源，没有需连接的数据记录
//		if (tags.length < 2)
//			return null;
//		String joinedData = "";
//		for (int i = 0; i < values.length; i++) {
//			if (i > 0)
//				joinedData += ",";
//			TaggedRecordWritable trw = (TaggedRecordWritable) values[i];
//			String recordLine = ((TaggedMapOutput) trw).getData().toString();
//			// 把CustomerID与后部的字段分为两段
//			String[] tokens = recordLine.split(",", 2);
//			// 拼接一次CustomerID
//			if (i == 0) {
//				joinedData += tokens[0];
//				// 拼接每个数据源记录后部的字段
//				joinedData += tokens[1];
//			}
//		}
//		TaggedRecordWritable retv = new TaggedRecordWritable(new Text(joinedData));
//		// 把第一个数据源标签设为join后记录的标签
//		retv.setTag((Text) tags[0]);
//		// join后的该数据记录将在reduce()中雨GroupKey一起输出
//		return retv;
//	}
//
//}
