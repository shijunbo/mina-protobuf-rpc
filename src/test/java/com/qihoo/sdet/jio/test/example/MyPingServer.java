package com.qihoo.sdet.jio.test.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.server.TcpServer;
import com.qihoo.sdet.jio.test.example.PingPong.*;
import com.qihoo.sdet.jio.test.example.PingPong.PingService.BlockingInterface;

public class MyPingServer extends TcpServer<PongService> implements PongService.Interface {
	private static final Logger LOGGER = LoggerFactory.getLogger(MyPingServer.class);
	
	@Override
	public void pong(RpcController controller, Pong request,
			RpcCallback<Ping> done) {
		
		LOGGER.debug("PongServiceImpl call now !!!!");
		Ping response = Ping.newBuilder().setNumber(request.getNumber()).setPingData(request.getPongData()).build();
		done.run(response);
	}
	
	
	public void sessionClosed(RpcClientChannel rpcChannel){
		LOGGER.debug("sessionClosed ..." + rpcChannel.getSession());
	}
	
	
	public static void synPingService(RpcClientChannel rpcChannel){
		try {
			BlockingInterface serverService = PingService.newBlockingStub(rpcChannel);
			Ping serverRequest = Ping.newBuilder().setNumber(2).setPingData(ByteString.copyFrom(new byte[5])).build();
			serverService.ping(rpcChannel.newRpcController(), serverRequest);
			LOGGER.debug("synPingService ping call success!!!");
		} catch ( Exception e ) {
			e.printStackTrace();
			LOGGER.error("synPingService ping call failed: " + e.getMessage());
		}
	}
	
	public static void asynPingService(RpcClientChannel rpcChannel){
		try {
			PingService serverService = PingService.newStub(rpcChannel);
			Ping serverRequest = Ping.newBuilder().setNumber(2).setPingData(ByteString.copyFrom(new byte[5])).build();
			LOGGER.debug("asynPingService ping call sending......");
			RpcController r = rpcChannel.newRpcController();
			serverService.ping(r, serverRequest, new RpcCallbackDone<Pong>(r) );
			LOGGER.debug("asynPingService ping call send ok");
		} catch ( Exception e ) {
			e.printStackTrace();
			LOGGER.error("asynPingService ping call failed: " + e.getMessage());
		}
	}
	
	
	public static void asynPongService(RpcClientChannel rpcChannel){
		try {
			PongService serverService = PongService.newStub(rpcChannel);
			Pong clientRequest = Pong.newBuilder().setNumber(1).setPongData(ByteString.copyFrom(new byte[5])).build();
			LOGGER.info("asynPongService pong call sending......");
			RpcController r = rpcChannel.newRpcController();
			serverService.pong(r, clientRequest, new RpcCallbackDone<Ping>(r) );
			LOGGER.info("asynPongService pong call send ok");
		} catch ( Exception e ) {
			e.printStackTrace();
			LOGGER.error("asynPongService call failed: " + e.getMessage());
		}
	}
	
	public void sessionOpen(RpcClientChannel rpcChannel){
		LOGGER.debug("sessionOpen ..."+ rpcChannel.getSession().getRemoteAddress());
		asynPingService(rpcChannel);
		asynPongService(rpcChannel);
	}

}
