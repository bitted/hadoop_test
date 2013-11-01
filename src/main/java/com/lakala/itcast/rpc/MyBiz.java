package com.lakala.itcast.rpc;

import java.io.IOException;

public class MyBiz implements MyBizable{
	public static final Long VERSION = 345L;
	/* (non-Javadoc)
	 * @see rpc.MyBizable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name){
		System.out.println("被调用了");
		return "hello "+name;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return VERSION;
	}
}
