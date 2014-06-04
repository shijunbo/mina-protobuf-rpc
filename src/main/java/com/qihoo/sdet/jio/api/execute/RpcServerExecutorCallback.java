package com.qihoo.sdet.jio.api.execute;

import com.google.protobuf.Message;

public interface RpcServerExecutorCallback {

	public void onFinish(int correlationId, Message message);
}
