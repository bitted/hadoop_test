package com.lakala.itcast.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {

	public static void main(String[] args) throws Exception {
		// final MyBizable proxy = (MyBizable) RPC.getProxy(MyBizable.class, MyBiz.VERSION, new InetSocketAddress(
		// MyServer.SERVER_ADDRESS, MyServer.SERVER_PORT), new Configuration());
		// final String result = proxy.hello("wuchao");
		// System.out.println(result);
		// RPC.stopProxy(proxy);

		final MyBizable proxy = (MyBizable) RPC.getProxy(MyBizable.class, MyBiz.VERSION, new InetSocketAddress(MyServer.SERVER_ADDRESS,
				MyServer.SERVER_PORT), new Configuration());
		
		final String result = proxy.hello("RPC");
		System.out.println("result =" +result);
		RPC.stopProxy(proxy);
		
	}
}
