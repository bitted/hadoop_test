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
 * 类名称：upload
 * 类描述：(通过FileSystem.copyFromLocalFile（src,dst），将本地文件上传到HDFS指定位置，其中path为文件的完整路径。)
 * 创建人：litj
 * 创建时间：2013年10月14日 下午3:01:13
 * 修改人：
 * 修改时间：2013年10月14日 下午3:01:13
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class Upload {

	/**
	 * main(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem hdfs = getFileSystem();// FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);

		Path src = new Path("F:\\lkl-mini-client-core-1.4.1.jar");
		// Path src = new Path("F:\\hadoop\\1.txt");
		// hdfs.mkdirs(new Path("/big_data_path/2/"));
		Path dst = new Path("/big_data_path");
		hdfs.copyFromLocalFile(false, src, dst);// true为递归删除
		System.out.println("Upload to " + conf.get("fs.default.name"));

		FileStatus files[] = hdfs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
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
