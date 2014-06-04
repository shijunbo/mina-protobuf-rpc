package com.qihoo.sdet.jio.api.execute;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.RpcCallback;
import com.qihoo.sdet.jio.api.core.RpcClient;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;

public class ServerRpcController implements com.google.protobuf.RpcController {

	private String failureReason;
	private final AtomicBoolean canceled = new AtomicBoolean(false);
	private final AtomicBoolean cancelCallbackNotified = new AtomicBoolean(
			false);
	private RpcCallback<Object> cancelNotifyCallback;
	private RpcClient rpcClient;
	private int correlationId;

	public ServerRpcController(RpcClient rpcClient, int correlationId) {
		this.rpcClient = rpcClient;
		this.correlationId = correlationId;
	}

	public static RpcClientChannel getRpcChannel(
			com.google.protobuf.RpcController controller) {
		ServerRpcController c = (ServerRpcController) controller;
		return c.getRpcClient();
	}

	public String errorText() {
		throw new IllegalStateException("Client-side use only.");
	}

	public boolean failed() {
		throw new IllegalStateException("Client-side use only.");
	}

	public void startCancel() {
		this.canceled.set(true);
	}

	public boolean isCanceled() {
		return canceled.get();
	}

	public void notifyOnCancel(RpcCallback<Object> callback) {
		if (isCanceled()) {
			callback.run(null);
		} else {
			cancelNotifyCallback = callback;
		}
	}

	public void setFailed(String reason) {
		this.failureReason = reason;
	}

	public void reset() {
		throw new IllegalStateException("Client-side use only.");
	}

	public String getFailed() {
		return failureReason;
	}

	public RpcCallback<Object> getCancelNotifyCallback() {
		return cancelNotifyCallback;
	}

	public int getCorrelationId() {
		return correlationId;
	}

	public RpcClient getRpcClient() {
		return rpcClient;
	}

	public boolean getAndSetCancelCallbackNotified() {
		return cancelCallbackNotified.getAndSet(true);
	}

}