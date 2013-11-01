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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 类名称：FileBlockLocation
 * 类描述：(通过FileSystem.getFileBlockLocation（file,start,len）可查找指定文件在hdfs集群上的位置，
 * 其中file为完整路径，start和len来标识查找文件的路径。)
 * 创建人：litj
 * 创建时间：2013年10月14日 下午3:01:13
 * 修改人：
 * 修改时间：2013年10月14日 下午3:01:13
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class FileBlockLocation {

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

		FileSystem hdfs = getFileSystem();// FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);
		FileStatus status = hdfs.getFileStatus(new Path("/big_data_path/2"));

		BlockLocation[] localtion = hdfs.getFileBlockLocations(status, 0, status.getLen());
		System.out.println(localtion.length);
		for (int i = 0; i < localtion.length; i++) {
			String[] hosts = localtion[i].getHosts();
			System.out.println("==block:" + i + ",localtion:" + hosts[i]);
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
