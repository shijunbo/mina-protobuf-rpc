package com.qihoo.sdet.jio.api.execute;

public interface RpcServerCallExecutor {

	public void execute(PendingServerCallState call);

	public void cancel(Runnable executor);

	public void shutdown();
}
