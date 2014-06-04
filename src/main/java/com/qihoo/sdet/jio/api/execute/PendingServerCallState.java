package com.qihoo.sdet.jio.api.execute;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;

public class PendingServerCallState {

	private final RpcServerExecutorCallback executorCallback;
	private final Service service;
	private final ServerRpcController controller;
	private final MethodDescriptor methodDesc;
	private final Message request;
	private final long startTS;

	private CallRunner executor;

	public PendingServerCallState(RpcServerExecutorCallback executorCallback,
			Service service, ServerRpcController controller,
			MethodDescriptor methodDesc, Message request, long startTS) {
		this.executorCallback = executorCallback;
		this.service = service;
		this.controller = controller;
		this.methodDesc = methodDesc;
		this.request = request;
		this.startTS = startTS;
	}

	public ServerRpcController getController() {
		return controller;
	}

	public Message getRequest() {
		return request;
	}

	public long getStartTS() {
		return startTS;
	}

	public MethodDescriptor getMethodDesc() {
		return methodDesc;
	}

	public CallRunner getExecutor() {
		return executor;
	}

	public void setExecutor(CallRunner executor) {
		this.executor = executor;
	}

	public Service getService() {
		return service;
	}

	public RpcServerExecutorCallback getExecutorCallback() {
		return executorCallback;
	}

}
