/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala
 * 文件名：BasePath.java
 * 版本信息：V1.0
 * 日期：2013年10月8日-下午4:53:49
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 类名称：BasePath
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月8日 下午4:53:49
 * 修改人：
 * 修改时间：2013年10月8日 下午4:53:49
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class BasePath {
	public static final String HADOOP_URL = "hdfs://10.5.12.66:9000";

	/**
	 * deleteOutputDirectory(删除输出目录，因为hadoop的输出目录必须是不存在的才能够输出)
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws URISyntaxException
	 *             返回类型：void
	 * @exception
	 * @since 1.0.0
	 */
	public static void deleteOutputDirectory(final Configuration conf,String OUTPUT_URL) throws IOException, URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URL), conf);

		/**
		 * 删除一个目录，设置为true表示递归删除一个目录
		 */
		fileSystem.delete(new Path(OUTPUT_URL), true);
	}
}
