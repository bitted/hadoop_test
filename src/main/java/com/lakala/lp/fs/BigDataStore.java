/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.fs
 * 文件名：BigDataStore.java
 * 版本信息：V1.0
 * 日期：2013年10月14日-下午2:30:28
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.fs;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 类名称：BigDataStore
 * 类描述：(通过FileSystem.copyFromLocalFile（src,dst），将本地文件上传到HDFS指定位置，其中path为文件的完整路径。)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月14日 下午2:30:28
 * 修改人：
 * 修改时间：2013年10月14日 下午2:30:28
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class BigDataStore {
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
		/**
		 * 1、确定需要上传视频流路径和接受视频流路径
		 */
		FileSystem hdfs = getFileSystem(); // FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		/**
		 * 2、输入的文件路径
		 */
		Path inputPath = new Path("F:\\hadoop");
		System.out.println("==inputPath==" + inputPath.toString());
		/**
		 * 3、存放到hdfs的文件目录
		 */
		Path hdfsPath = new Path("/big_data_path/");
		/**
		 * 4、在HDFS上创建big_data_path目录，存放大数据文件
		 */
		hdfs.mkdirs(hdfsPath);
		/**
		 * 5、获取本地需要上传的文件列表
		 */
		FileStatus[] inputFiles = local.listStatus(inputPath);
		/**
		 * 6、通过OutputStream.write()来将文件循环写入HDFS指定目录
		 */
		FSDataOutputStream out;
		for (int i = 0; i < inputFiles.length; i++) {
			System.out.println("<第" + i + "的文件名称" + inputFiles[i].getPath().getName() + ">");
			/**
			 * 7、获取文件目录
			 */
			FSDataInputStream in = local.open(inputFiles[i].getPath());
			out = hdfs.create(new Path("/big_data_path/" + inputFiles[i].getPath().getName()));

			byte buffer[] = new byte[256];
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytesRead);
			}
			out.close();
			in.close();
			System.out.println("======上传成功！=======");
			/**
			 * 8、上传到HDFS成功后，删除本地缓存区文件。
			 */
			File file = new File(inputFiles[i].getPath().toString());
			file.delete();
			System.out.println("======删除缓存区数据成功！=======");
		}
	}

	private static FileSystem getFileSystem() throws Exception {
		final Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		final FileSystem fs = FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);
		return fs;
	}
}
