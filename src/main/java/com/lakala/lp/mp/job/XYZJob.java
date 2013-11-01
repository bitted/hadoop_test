/**
 * 项目名称：拉卡拉手机钱包
 * 包名：com.lakala.lp.mp.job
 * 文件名：XYZJob.java
 * 版本信息：V1.0
 * 日期：2013年10月22日-下午1:59:00
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

/**
 * 类名称：XYZJob
 * 类描述：(具有复杂依赖关系的组合式MapR
 * import org.apache.hadoop.conf.Configuration;
 * educe作业)
 * 创建人：litj
 * 创建时间：2013年10月22日 下午1:59:00
 * 修改人：
 * 修改时间：2013年10月22日 下午1:59:00
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class XYZJob {

	/**
	 * main(这里用一句话描述这个方法的作用)
	 * (这里描述这个方法适用条件 – 可选)
	 * 
	 * @param args
	 *            返回类型：void
	 * @throws IOException
	 * @exception
	 * @since 1.0.0
	 */

	public static void main(String[] args) throws IOException {
		// 1、 配置jobx
		Configuration jobxConf = new Configuration();
		JobConf conf1 = new JobConf(jobxConf);
		Job jobx = new Job(conf1);
		jobx.setJobName("jobx");
		// ...配置jobx的其他输入、出类等配置

		// 2、 配置joby
		Configuration jobyConf = new Configuration();
		JobConf conf2 = new JobConf(jobyConf);
		Job joby = new Job(conf2);
		joby.setJobName("joby");
		// ...配置joby的其他输入、出类等配置

		// 3、 配置jobz
		Configuration jobzConf = new Configuration();
		JobConf conf3 = new JobConf(jobzConf);
		Job jobz = new Job(conf3);
		joby.setJobName("jobz");
		// ...配置jobz的其他输入、出类等配置

		// 4、设置jobz与jobx的依赖关系，jobz将等待jobx执行完毕
		jobz.addDependingJob(jobx);
		// 4、设置jobz与joby的依赖关系，jobz将等待joby执行完毕
		jobz.addDependingJob(joby);

		// 5、设置JobControl，把三个job加入JobControl
		JobControl jc = new JobControl("XYZjob");
		jc.addJob(jobx);
		jc.addJob(joby);
		jc.addJob(jobz);

		jc.run();// 启动这个包含3个子任务的整体作业执行过程
	}
}
