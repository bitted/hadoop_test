///**
// * 项目名称：拉卡拉手机钱包
// * 包名：com.lakala.lp.mp.custom
// * 文件名：TaggedRecordWritable.java
// * 版本信息：V1.0
// * 日期：2013年10月22日-下午4:27:02
// * 作者：litj
// * Copyright (c) 2013拉卡拉支付有限公司-版权所有
// */
//
//package com.lakala.lp.mp.custom.datajoin;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//
///**
// * 类名称：TaggedRecordWritable
// * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
// * (这里描述此类业务作用，描述性说明 ----必填）
// * 创建人：litj
// * 创建时间：2013年10月22日 下午4:27:02
// * 修改人：
// * 修改时间：2013年10月22日 下午4:27:02
// * 修改备注：
// * 
// * @version 1.0.0
// */
//
//public class TaggedRecordWritable extends TaggedMapOutput {
//
//	private Writable data;
//
//	/**
//	 * 创建一个新的实例 TaggedRecordWritable.
//	 * 
//	 * @param value
//	 */
//
//	public TaggedRecordWritable(Writable value) {
//		this.tag = new Text("");
//		this.data = value;
//	}
//
//	@Override
//	public void write(DataOutput out) throws IOException {
//		this.tag.write(out);
//		this.data.write(out);
//	}
//
//
//	@Override
//	public void readFields(DataInput in) throws IOException {
//		this.tag.readFields(in);
//		this.data.readFields(in);
//	}
//
//	@Override
//	public Writable getData() {
//		return data;
//	}
//
//}
