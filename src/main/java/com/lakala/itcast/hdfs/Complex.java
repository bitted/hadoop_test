/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.hdfs
 * 文件名：Complex.java
 * 版本信息：V1.0
 * 日期：2013年9月27日-上午10:20:02
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.itcast.hdfs;

import java.io.FileInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 类名称：Complex
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年9月27日 上午10:20:02
 * 修改人：
 * 修改时间：2013年9月27日 上午10:20:02
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class Complex {

	/**
	 * main(hadoop的FileSystem操作类)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws Exception {
		final FileSystem fs = getFileSystem();
		final String FILE_NAME = "/hello1";
		/**
		 * 1、写文件
		 */
		// writeFile(fs, FILE_NAME);
		/**
		 * 2、读文件
		 */
		// readFile(fs, FILE_NAME);
		/**
		 * 3、创建目录
		 */
		// mkdirs(fs);
		/**
		 * 4、列表
		 */
		// list(fs);
		/**
		 * 5、操作汇总
		 */
		opt(fs);
		System.out.println("==操作成功！==");

	}

	private static void opt(FileSystem fs) throws Exception {
		/**
		 * 1、copy本地文件到hdfs中
		 */
		// fs.copyFromLocalFile(new Path("F:/es.log"),new Path("/f/log.log"));
		/**
		 * 2、copy本地文件到hdfs中,完成后删除本地文件
		 */
		// fs.copyFromLocalFile(true, new Path("F:/es.log"), new Path("/f/log2.log"));
		/**
		 * 3、copy本地文件到hdfs中,不删除本地文件，重写hdfs文件
		 */
		// fs.copyFromLocalFile(false, true, new Path("F:/es.log"), new Path("/f/log2.log"));
		/**
		 * 4、copy HDFS中文件到本地
		 */
		// fs.copyFromLocalFile(false, true, new Path[], new Path("/f/log2.log"));
		// fs.copyToLocalFile(new Path("/"), new Path("F:/dfs"));
		// fs.copyToLocalFile(new Path("/input/log.txt"), new Path("F:/"));
		/**
		 * 5、copy到本地后删除dfs上面的文件
		 */
		fs.copyToLocalFile(true, new Path("/input/"), new Path("F:/a"));
	}

	/**
	 * list(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param fs
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	private static void list(FileSystem fs) throws Exception {
		final FileStatus[] list = fs.listStatus(new Path("/"));
		for (FileStatus status : list) {
			String isDir = status.isDir() ? "目录" : "文件";
			final Path path = status.getPath();
			final long len = status.getLen();
			System.out.println(isDir + "\t" + len + "\t" + path.toString());
		}
	}

	/**
	 * mkdirs(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param fs
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	private static void mkdirs(FileSystem fs) throws Exception {
		fs.mkdirs(new Path("/d2"));
	}

	/**
	 * readFile(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param fs
	 * @param file_name
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	private static void readFile(FileSystem fs, String file_name) throws Exception {
		final FSDataInputStream in = fs.open(new Path(file_name));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	/**
	 * writeFile(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param fs
	 * @param file_name
	 * @throws Exception
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	private static void writeFile(FileSystem fs, String file_name) throws Exception {
		final FSDataOutputStream out = fs.create(new Path(file_name));
		final FileInputStream in = new FileInputStream("F:/es.log");
		IOUtils.copyBytes(in, out, 1024, true);
	}

	/**
	 * getFileSystem(返回fs)
	 * 
	 * @return
	 * @throws Exception
	 *             返回类型：FileSystem
	 * @exception
	 * @since 1.0.0
	 */
	private static FileSystem getFileSystem() throws Exception {
		final Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		final FileSystem fs = FileSystem.get(new URI("hdfs://10.5.12.66:9000"), conf);
		return fs;
	}
}
