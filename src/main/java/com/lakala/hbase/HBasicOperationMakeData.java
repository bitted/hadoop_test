/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.hbase
 * 文件名：HBasicOperation.java
 * 版本信息：V1.0
 * 日期：2013年10月16日-下午2:06:17
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.hbase;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * 类名称：HBasicOperation
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * 创建人：litj
 * 创建时间：2013年10月16日 下午2:06:17
 * 修改人：
 * 修改时间：2013年10月16日 下午2:06:17
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class HBasicOperationMakeData {

	public static final String tab = "people";

	/**
	 * getHTable(获取HTable，沒有就创建table)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param conf
	 * @param admin
	 * @return
	 * @throws Exception
	 *             返回类型：HTable
	 * @exception
	 * @since 1.0.0
	 */
	public static HTable getHTable(Configuration conf, HBaseAdmin admin) throws Exception {
		System.out.println("=====获取table对象=========");

		/**
		 * 首先检查table表是否可用，不可用的时候创建该表
		 */
		HTableDescriptor tableDescriptor = new HTableDescriptor(tab.getBytes());
		if (!admin.isTableAvailable(tab.getBytes())) {
			tableDescriptor.addFamily(new HColumnDescriptor("attributes"));
			admin.createTable(tableDescriptor);
		}
		HTable table = new HTable(conf, tab);
		return table;
	}

	public static HTable addFamily(Configuration conf, HBaseAdmin admin) throws Exception {
		System.out.println("=====增加列族=========");
		/**
		 * 首先检查table表是否可用，不可用的时候创建该表
		 */
		HTableDescriptor tableDescriptor = new HTableDescriptor(tab.getBytes());
		if (!tableDescriptor.hasFamily("attributes".getBytes())) {
			if (admin.isTableEnabled(tab)) {
				admin.disableTable(tab);
			}
			admin.addColumn(tab.getBytes(), new HColumnDescriptor("attributes"));
		}
		HTable table = new HTable(conf, tab);
		return table;
	}

	/**
	 * put(向table中插入数据)
	 * 
	 * @param table
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void put(HTable table) throws Exception {
		System.out.println("=====向table中插入数据=========");
		for (int i = 0; i < 10; i++) {
			Put putRow1 = new Put(("row" + i).getBytes());
			putRow1.add(("attributes").getBytes(), ("name").getBytes(), ("jock-" + i).getBytes());
			putRow1.add(("attributes").getBytes(), ("email").getBytes(), ("email-" + i).getBytes());
			putRow1.add(("attributes").getBytes(), ("phone").getBytes(), ("1510107601" + i).getBytes());
			table.put(putRow1);
		}
		System.out.println("add table data success！");
	}

	public static void switchTable(HBaseAdmin admin, HTable table) throws Exception {
		if (admin.isTableEnabled(tab)) {
			admin.disableTable(tab);
			System.out.println("=====disableTable=========");
		} else {
			admin.enableTable(tab);
			System.out.println("=====enableTable=========");
		}
	}

	/**
	 * putOne(更新一条记录)
	 * 
	 * @param table
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void putOne(HBaseAdmin admin, HTable table) throws Exception {
		System.out.println("=====更新一条记录=========");
		if (admin.isTableDisabled(tab)) {
			admin.enableTable(tab);
			System.out.println("=====enableTable=========");
		}
		Put putRow1 = new Put(("row" + 1).getBytes());
		// putRow1.add(("fam1").getBytes(), ("col" + 2).getBytes(), ("value" + 3).getBytes());
		putRow1.add(("fam2").getBytes(), ("col" + 2).getBytes(), ("value" + 3).getBytes());
		putRow1.add(("fam2").getBytes(), ("name" + 2).getBytes(), ("lisi" + 3).getBytes());
		table.put(putRow1);
		System.out.println("add table data success！");
	}

	/**
	 * scanner(扫描一个列族的数据)
	 * 
	 * @param table
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void scanner(HTable table) throws Exception {
		System.out.println("=====扫描一个列族的数据=========");
		for (Result row : table.getScanner("attributes".getBytes())) {
			System.out.format("ROW\t%s\n", new String(row.getRow()));

			for (Map.Entry<byte[], byte[]> entry : row.getFamilyMap("attributes".getBytes()).entrySet()) {
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.format("COLUMN\tfam1:%s\t%s\n", column, value);

			}
		}
	}

	/**
	 * query(按照列族中的列查询一条记录)
	 * 
	 * @param table
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void query(HBaseAdmin admin, HTable table, String familyCulumn, String clum) throws Exception {
		if (admin.isTableDisabled(tab)) {
			admin.enableTable(tab);
			System.out.println("=====enableTable=========");
		}
		System.out.println("=====按照列族中的列查询一条记录=========");
		for (Result row : table.getScanner(familyCulumn.getBytes(), clum.getBytes())) {
			System.out.format("ROW\t%s\n", new String(row.getRow()));
			for (Map.Entry<byte[], byte[]> entry : row.getFamilyMap(familyCulumn.getBytes()).entrySet()) {
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.format("COLUMN\tattributes:%s\t%s\n", column, value);
			}
		}
	}

	/**
	 * delete(删除表，先disable，再delete)
	 * 
	 * @param admin
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void delete(HBaseAdmin admin) throws Exception {
		admin.disableTable(tab);
		admin.deleteTable(tab);
		System.out.println("======删除成功！======");
	}

	/**
	 * deleteColumn(删除表中的一列)
	 * 
	 * @param admin
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void deleteColumn(HBaseAdmin admin, HTable table, String tab, String column) throws Exception {
		System.out.println("======删除表中的一列======");

		switchTable(admin, table);
		if (admin.getTableDescriptor(tab.getBytes()).hasFamily(column.getBytes())) {
			admin.deleteColumn(tab, column);
			System.out.println("======删除列成功！======");
		} else {
			System.out.println("======不存在该列！======");

		}
	}

	/**
	 * main(这里用一句话描述这个方法的作用)
	 * (出现错误：java.net.ConnectException: Connection refused: no further information
	 * 可能是防火牆的原因
	 * )
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		/**
		 * 1 、分布式环境设置zookeeper
		 */
		conf.set("hbase.zookeeper.quorum", "10.5.11.57");
		HBaseAdmin admin = new HBaseAdmin(conf);
		/**
		 * 2、获取table
		 */
		HTable table = getHTable(conf, admin);
		// HTable table = addFamily(conf, admin);
		/**
		 * 3、table插入数据
		 */
		put(table);
		// putOne(admin, table);
		/**
		 * 4、扫描一个列族的数据
		 */
		// scanner(table);
		/**
		 * 5、刪除表
		 */
		// delete(admin);
		// deleteColumn(admin, table, tab, "fam2");
		/**
		 * 6、按照列族中的列查询一条记录,默认返回时间戳最近的一条记录
		 */
		query(admin, table, "attributes", "name");
	}
}
