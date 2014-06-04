package com.qihoo.sdet.jio.api.listener;

import com.qihoo.sdet.jio.api.core.RpcClientChannel;

public interface TcpConnectionEventListener {

	public void connectionClosed(RpcClientChannel clientChannel);

	public void connectionOpened(RpcClientChannel clientChannel);

}
