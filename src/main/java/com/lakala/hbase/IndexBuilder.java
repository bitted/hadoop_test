/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.hbase
 * 文件名：IndexBuilder.java
 * 版本信息：V1.0
 * 日期：2013年10月18日-上午9:49:34
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 类名称：IndexBuilder
 * 类描述：(构建索引表)
 * (运行程序需要设置运行参数，分别为表名，列族和需要索引的列【列必须属于前面给出的列族】，如果对heroes中的name和email列构建索引，
 * 则运行参数应设置为：people attributes name email phone)
 * 创建人：litj
 * 创建时间：2013年10月18日 上午9:49:34
 * 修改人：
 * 修改时间：2013年10月18日 上午9:49:34
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class IndexBuilder {

	// 索引表唯一的一列为INDEX：ROW，其中INDEX为列族
	public static final byte[] INDEX_COLUMN = Bytes.toBytes("INDEX");

	public static final byte[] INDEX_QUALIFIER = Bytes.toBytes("ROW");

	/**
	 * 类名称：Map
	 * 类描述：(实现一张表数据量大时，按照列族创建索引表的操作。)
	 * 创建人：litj
	 * 创建时间：2013年10月21日 下午2:03:24
	 * 修改人：
	 * 修改时间：2013年10月21日 下午2:03:24
	 * 修改备注：
	 * 
	 * @version 1.0.0
	 */
	public static class Map extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Writable> {
		private byte[] family;

		/**
		 * 存储了 '列名'到'表名-列名'的映射
		 * 谴责用于获取某列的值，并作为索引表的键值；侯泽用户作为索引表的表名
		 */
		private HashMap<byte[], ImmutableBytesWritable> indexes;

		/**
		 * 表：【people-name】
		 * 创建索引后的数据格式
		 * jock-0 column=INDEX:ROW, timestamp=1382334583437, value=row0
		 * jock-1 column=INDEX:ROW, timestamp=1382334583437, value=row1
		 * jock-2 column=INDEX:ROW, timestamp=1382334583437, value=row2
		 * jock-3 column=INDEX:ROW, timestamp=1382334583437, value=row3
		 * jock-4 column=INDEX:ROW, timestamp=1382334583437, value=row4
		 * jock-5 column=INDEX:ROW, timestamp=1382334583437, value=row5
		 * jock-6 column=INDEX:ROW, timestamp=1382334583437, value=row6
		 * jock-7 column=INDEX:ROW, timestamp=1382334583437, value=row7
		 * jock-8 column=INDEX:ROW, timestamp=1382334583437, value=row8
		 * jock-9 column=INDEX:ROW, timestamp=1382334583437, value=row9
		 * 
		 */
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException,
				InterruptedException {
			// System.out.println("=========into map()===============" + rowKey);
			for (java.util.Map.Entry<byte[], ImmutableBytesWritable> index : indexes.entrySet()) {

				byte[] qualifier = index.getKey();// 获取列名
				ImmutableBytesWritable tableName = index.getValue();// 索引表的表名
				byte[] value = result.getValue(family, qualifier);// 根据‘列族：列名’获取元素值

				if (value != null) {
					// 以列值作为行键，在列“INDEX:ROW”中插入行键
					Put put = new Put(value);
					put.add(INDEX_COLUMN, INDEX_QUALIFIER, rowKey.get());

					/**
					 * tableName表上执行put操作
					 * 使用MultiOutputFormat时，第二个参数必须是Put或则Delete类型
					 */
					context.write(tableName, put);
				}
			}
			// System.out.println("=========out map() success===============");
		}

		public static HTable getHTable(Configuration conf, HBaseAdmin admin, String tab) throws Exception {
			/**
			 * 首先检查table表是否可用，不可用的时候创建该表
			 */
			HTableDescriptor tableDescriptor = new HTableDescriptor(tab.getBytes());
			if (!admin.isTableAvailable(tab.getBytes())) {
				tableDescriptor.addFamily(new HColumnDescriptor(INDEX_COLUMN));
				admin.createTable(tableDescriptor);
			}
			HTable table = new HTable(conf, tab);
			return table;
		}

		/**
		 * 2、setup为Mapper中的方法，该方法值在任务初始化时执行一次
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			// System.out.println("=========into setup()===============");

			Configuration conf = context.getConfiguration();
			// 通过conf.set() 方法传递参数，详细见下面configureJob方法
			String tableName = conf.get("index.tablename");
			String[] fields = conf.getStrings("index.fields");

			// fields内为需要做索引的列名
			String familyName = conf.get("index.familyname");
			family = Bytes.toBytes(familyName);

			// 初始化indexs方法
			indexes = new HashMap<byte[], ImmutableBytesWritable>();
			for (String field : fields) {
				/**
				 * 生成索引表
				 */
				String newTable = tableName + "-" + field;
				System.out.println("=========into setup()=======tableName=" + tableName + ",===field=" + field);
				HBaseAdmin admin = new HBaseAdmin(context.getConfiguration());

				try {
					getHTable(context.getConfiguration(), admin, newTable);
					System.out.println("====创建表成功！======");
				} catch (Exception e) {
					e.printStackTrace();
				}
				// 如果给name做索引，则索引表的名称为‘heroes-name’
				indexes.put(Bytes.toBytes(field), new ImmutableBytesWritable(Bytes.toBytes(newTable)));
			}
		}
	}

	/**
	 * convertScanToString(copy过来的方法)
	 * 
	 * @param scan
	 * @return
	 * @throws IOException
	 *             返回类型：String
	 * @exception
	 * @since 1.0.0
	 */
	static String convertScanToString(Scan scan) throws IOException {
		// System.out.println("=========into convertScanToString()===============");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}

	/**
	 * configureJob(获取参数，封装map需要的数据，格式化)
	 * 
	 * @param conf
	 * @param args
	 * @return
	 * @throws IOException
	 *             返回类型：Job
	 * @exception
	 * @since 1.0.0
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		// System.out.println("=========into configureJob()===============");

		String tableName = args[0];
		String columnFamily = args[1];
		// System.out.println("======tableName=" + tableName + ",columnFamily=" + columnFamily);
		conf.set("hbase.zookeeper.quorum", "10.5.11.57");

		// 通过Configuration.set()方法传递参数
		conf.set(TableInputFormat.SCAN, convertScanToString(new Scan()));
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set("index.tablename", tableName);
		conf.set("index.familyname", columnFamily);

		String[] fields = new String[args.length - 2];

		for (int i = 0; i < fields.length; i++) {
			fields[i] = args[i + 2];
		}

		conf.setStrings("index.fields", fields);
		conf.set("index.familyname", "attributes");

		// 配置任务的运行参数
		Job job = new Job(conf, tableName);
		job.setJarByClass(IndexBuilder.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);

		return job;

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = HBaseConfiguration.create();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Only " + otherArgs.length + " arguments supplied, required: 3");
			System.err.println("Usage: IndexBuilder <TABLE_NAME> <COLUMN_FAMILY> <ATTR> [<ATTR> ...]");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
