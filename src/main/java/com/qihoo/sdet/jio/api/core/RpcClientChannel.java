package com.qihoo.sdet.jio.api.core;

import org.apache.mina.core.session.IoSession;

import com.google.protobuf.RpcController;
import com.qihoo.sdet.jio.api.stream.IStreamReady;

public interface RpcClientChannel extends com.google.protobuf.RpcChannel,
		com.google.protobuf.BlockingRpcChannel {

	public Bootstrap getBootstrap();

	public PeerInfo getPeerInfo();
	
	public PeerInfo getLocalInfo();

	public RpcController newRpcController();

	public void close();

	public IoSession getChannel();
	
	public IoSession getSession();
	
	public void stream(String path, IStreamReady ready);
	
	public String synStream(String path) throws Exception;
	
	public boolean reconnect();
}
