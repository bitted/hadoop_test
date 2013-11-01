/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp.custom
 * 文件名：DataJoin.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-下午4:10:58
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom.datajoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 类名称：DataJoin
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * 创建人：litj
 * 创建时间：2013年10月22日 下午4:10:58
 * 修改人：
 * 修改时间：2013年10月22日 下午4:10:58
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class DataJoin {

	public static class MapClass extends DataJoinMapperBase {

		@Override
		protected Text generateInputTag(String inputFile) {
			// 使用输入文件名作为标签
			String dataSource = inputFile.split("-")[0];
			// 该数据源标签将被map()保存在inputTag中
			return new Text(dataSource);
		}

		@Override
		protected TaggedMapOutput generateTaggedMapOutput(Object value) {
			TaggedRecordWritable record = new TaggedRecordWritable((Text) value);
			// 把一个原始数据记录包装为标签化的记录
			record.setTag(this.inputTag);
			return record;
		}

		@Override
		protected Text generateGroupKey(TaggedMapOutput aRecord) {
			String line = aRecord.getData().toString();
			String[] tokens = line.split(",");
			String groupKey = tokens[0];
			// 取CustomerID作为GroupKey(key)
			return new Text(groupKey);
		}

	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, DataJoin.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		// FileInputFormat.setInputPaths(job, in);
		// FileOutputFormat.setOutputPath(job, out);

		job.setJobName("DataJoin");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reducer.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TaggedRecordWritable.class);
		job.set("mapred.textoutputformat.separator", ",");
		// job.waitForCompletion(true);

	}

	public static class ReduceClass extends DataJoinReducerBase {

		@Override
		protected TaggedMapOutput combine(Object[] tags, Object[] values) {

			// 一个以下数据源，没有需连接的数据记录
			if (tags.length < 2)
				return null;
			String joinedData = "";
			for (int i = 0; i < values.length; i++) {
				if (i > 0)
					joinedData += ",";
				TaggedRecordWritable trw = (TaggedRecordWritable) values[i];
				String recordLine = ((TaggedMapOutput) trw).getData().toString();
				// 把CustomerID与后部的字段分为两段
				String[] tokens = recordLine.split(",", 2);
				// 拼接一次CustomerID
				if (i == 0) {
					joinedData += tokens[0];
					// 拼接每个数据源记录后部的字段
					joinedData += tokens[1];
				}
			}
			TaggedRecordWritable retv = new TaggedRecordWritable(new Text(joinedData));
			// 把第一个数据源标签设为join后记录的标签
			retv.setTag((Text) tags[0]);
			// join后的该数据记录将在reduce()中雨GroupKey一起输出
			return retv;
		}

	}

	public static class TaggedRecordWritable extends TaggedMapOutput {

		private Writable data;

		/**
		 * 创建一个新的实例 TaggedRecordWritable.
		 * 
		 * @param value
		 */

		public TaggedRecordWritable(Writable value) {
			this.tag = new Text("");
			this.data = value;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.tag.write(out);
			this.data.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.tag.readFields(in);
			this.data.readFields(in);
		}

		@Override
		public Writable getData() {
			return data;
		}
	}
}
