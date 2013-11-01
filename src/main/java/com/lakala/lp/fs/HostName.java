/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.fs
 * 文件名：upload.java
 * 版本信息：V1.0
 * 日期：2013年10月14日-下午3:01:13
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.fs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * 类名称：HostName
 * 类描述：(通过DatanodeInfo.getHostName,可获取HDFS集群上的所有节点名称。)
 * 创建人：litj
 * 创建时间：2013年10月14日 下午3:01:13
 * 修改人：
 * 修改时间：2013年10月14日 下午3:01:13
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class HostName {

	/**
	 * main(没有成功输出！)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {

		FileSystem fs = getFileSystem();
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] nodes = hdfs.getDataNodeStats();
		String[] names = new String[nodes.length];
		for (int i = 0; i < names.length; i++) {
			names[i] = nodes[i].getHostName();
			System.out.println("node=" + i + "\nname=" + names[i]);
		}

		System.out.println("OK!");
	}

	private static FileSystem getFileSystem() throws Exception {
		final Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		final FileSystem fs = FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);
		return fs;
	}
}
