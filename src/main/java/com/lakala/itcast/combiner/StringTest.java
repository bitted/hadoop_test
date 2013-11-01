package com.lakala.itcast.combiner;

/**
 * 类名称：StringTest
 * 类描述：(描述此类所在架构中层次，如：某某服务接口实现、某某实体模型、某某工具类等----必填)
 * (这里描述此类业务作用，描述性说明 ----必填）
 * 创建人：litj
 * 创建时间：2013年10月15日 下午2:15:14
 * 修改人：
 * 修改时间：2013年10月15日 下午2:15:14
 * 修改备注：
 * 
 * @version 1.0.0
 */
public class StringTest {

	public static void main(String[] args) {
		String name = "hdfs://10.3.3.3.1:78989/dhhd/dsds";
		int colon = name.indexOf(":");
		System.out.println(colon);
		System.out.println(name.substring(0, colon));
	}

}
