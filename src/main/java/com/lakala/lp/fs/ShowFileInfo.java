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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 类名称：ShowFileInfo
 * 类描述：(通过FileStatus.rename（src,dst），获取FileStatus，查询文件的具体属性。)
 * 创建人：litj
 * 创建时间：2013年10月14日 下午3:01:13
 * 修改人：
 * 修改时间：2013年10月14日 下午3:01:13
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class ShowFileInfo {

	/**
	 * main(这里用一句话描述这个方法的作用)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {

		FileSystem hdfs = getFileSystem();// FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);

		FileStatus status = hdfs.getFileStatus(new Path("/big_data_path/2.txt"));
		System.out.println("==status=[" + status.getBlockSize() + " ," + status.getGroup() + " ," + status.getLen() + " ,"
				+ status.getModificationTime() + " ," + status.getOwner() + "," + status.getReplication() + " ,"
				+ status.getPath() + " ," + status.getPermission() + "]");
		System.out.println("OK!");
	}

	private static FileSystem getFileSystem() throws Exception {
		final Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		final FileSystem fs = FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);
		return fs;
	}
}
