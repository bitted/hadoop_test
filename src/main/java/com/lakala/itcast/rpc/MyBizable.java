package com.lakala.itcast.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol{

	public abstract String hello(String name);

}