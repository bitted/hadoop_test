/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.hdfs
 * 文件名：Simple.java
 * 版本信息：V1.0
 * 日期：2013年9月27日-下午5:06:09
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.itcast.hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 类名称：Simple
 * 类描述：(输出一个目录中的文件的内容到控制台)
 * 创建人：litj
 * 创建时间：2013年9月27日 下午5:06:09
 * 修改人：
 * 修改时间：2013年9月27日 下午5:06:09
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class Simple {
	public static final String HELLO_PATH = "hdfs://10.5.12.66:9000/hello/hello";

	/**
	 * main(输出一个目录中的文件的内容到控制台)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws Exception
	 * @throws MalformedURLException
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws MalformedURLException, Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final InputStream in = (new URL(HELLO_PATH)).openStream();
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
}
