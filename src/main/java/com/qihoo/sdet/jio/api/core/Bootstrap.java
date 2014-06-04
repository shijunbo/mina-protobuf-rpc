package com.qihoo.sdet.jio.api.core;

public interface Bootstrap {

	void sessionClosed(RpcClientChannel rpcChannel);

	void sessionOpen(RpcClientChannel rpcChannel);
}
