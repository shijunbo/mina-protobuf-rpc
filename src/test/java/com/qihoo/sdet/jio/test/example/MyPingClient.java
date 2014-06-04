package com.qihoo.sdet.jio.test.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.qihoo.sdet.jio.api.client.TcpClient;
import com.qihoo.sdet.jio.api.core.Config;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.test.example.PingPong.*;
import com.qihoo.sdet.jio.test.example.PingPong.PongService.BlockingInterface;


public class MyPingClient extends TcpClient<PingService> implements PingService.Interface {

	
	private static final Logger LOGGER = LoggerFactory.getLogger(MyPingClient.class);
	
	@Override
	public void ping(RpcController controller, Ping request, RpcCallback<Pong> done) {
		
		LOGGER.info("PingServiceImpl implent call now !!!!");
//		Pong response = Pong.newBuilder().setNumber(request.getNumber()).setPongData(request.getPingData()).build();
//		done.run(response);
		
		throw new UnsupportedOperationException();
	}
	
	public static void main(String[] args) throws Exception {
		
		Config.getInstance().loadConf("conf.xml");
		
		while(true){
			Thread.sleep(5000);
		}
	}
	
	public static void synPongService(RpcClientChannel rpcChannel){
		try {
			BlockingInterface serverService = PongService.newBlockingStub(rpcChannel);
			Pong clientRequest = Pong.newBuilder().setNumber(1).setPongData(ByteString.copyFrom(new byte[5])).build();
			serverService.pong(rpcChannel.newRpcController(), clientRequest);
			LOGGER.info("synPongService pong call success!!!");
		} catch ( Exception e ) {
			e.printStackTrace();
			LOGGER.info("synPongService call failed: " + e.getMessage());
		}
	}
	
	public static void asynPongService(RpcClientChannel rpcChannel){
		try {
			PongService serverService = PongService.newStub(rpcChannel);
			Pong clientRequest = Pong.newBuilder().setNumber(1).setPongData(ByteString.copyFrom(new byte[5])).build();
			LOGGER.info("asynPongService pong call sending......");
			RpcController r = rpcChannel.newRpcController();
			serverService.pong(r, clientRequest, new RpcCallbackDone<Ping>(r));
			LOGGER.info("asynPongService pong call send ok");
		} catch ( Exception e ) {
			e.printStackTrace();
			LOGGER.error("asynPongService call failed: " + e.getMessage());
		}
	}
	
	public void sessionClosed(RpcClientChannel rpcChannel){
		LOGGER.info("sessionClosed ..." + rpcChannel.getSession());
	}
	
	public void sessionOpen(RpcClientChannel rpcChannel){
		LOGGER.info("sessionOpen ..."+ rpcChannel.getSession());
		//asynPongService(rpcChannel);
	}
	
}
