package com.qihoo.sdet.jio.api.stream;

import com.google.protobuf.RpcCallback;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkResponse;

public class StreamCallback implements RpcCallback<ChunkResponse> {

	String streamId = null; 
	public StreamCallback(String _streamid) {
		streamId = _streamid;
	}
	
	public void run(ChunkResponse responseMessage) {
		if (responseMessage != null) {
			StreamHandler.getInstance().handler(responseMessage);
		}else{
			StreamHandler.getInstance().handler(streamId);
		}
	}

}
