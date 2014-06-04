package com.qihoo.sdet.jio.test.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class RpcCallbackDone<K extends com.google.protobuf.GeneratedMessage> implements RpcCallback<K> {

	private static final Logger LOGGER = LoggerFactory.getLogger(RpcCallbackDone.class);
	
	protected RpcController _r;
	
	public RpcCallbackDone( RpcController r ){
		_r = r;
	}
	
	@Override
	public void run(K parameter) {
		// TODO Auto-generated method stub
		LOGGER.debug("notfiy now!!!! resMsg: " + parameter + " failed:" + _r.failed() + " errorText: " + _r.errorText() );
	}
	
}
