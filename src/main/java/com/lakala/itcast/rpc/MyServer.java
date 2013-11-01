package com.lakala.itcast.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 * 1.RPC的调用实际是通过jdk的代理实现的
 * 2.远程被调用的对象必须实现一个接口，被调用的方法必须在接口中定义
 * 3.客户端拿到的是接口
 * 4.服务端启动的是一个java服务进程
 */
public class MyServer {

	public static final int SERVER_PORT = 12345;
	public static final String SERVER_ADDRESS = "localhost";

	public static void main(String[] args) throws Exception {
		/**
		 * 构造一个 RPC server.
		 * 
		 * @param instance
		 *            被调用的实例
		 * @param bindAddress
		 *            监听连接的地址
		 * @param port
		 *            监听连接的端口
		 * @param conf
		 *            配置实例
		 */
		final Server server = RPC.getServer(new MyBiz(), SERVER_ADDRESS, SERVER_PORT, new Configuration());
		server.start();
	}

}
