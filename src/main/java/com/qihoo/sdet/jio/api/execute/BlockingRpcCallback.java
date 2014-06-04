package com.qihoo.sdet.jio.api.execute;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

public class BlockingRpcCallback implements RpcCallback<Message> {

	private boolean done = false;
	private Message message;

	public void run(Message message) {
		this.message = message;
		synchronized (this) {
			done = true;
			notify();
		}
	}

	public Message getMessage() {
		return message;
	}

	public boolean isDone() {
		return done;
	}

}
