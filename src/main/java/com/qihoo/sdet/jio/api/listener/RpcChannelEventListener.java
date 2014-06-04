package com.qihoo.sdet.jio.api.listener;

import com.qihoo.sdet.jio.api.core.RpcClientChannel;

public interface RpcChannelEventListener {

	public void connectionLost(RpcClientChannel clientChannel);

	public void connectionOpened(RpcClientChannel clientChannel);

	public void connectionReestablished(RpcClientChannel clientChannel);

	public void connectionChanged(RpcClientChannel clientChannel);
}
