package com.qihoo.sdet.jio.api.execute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallRunner implements Runnable {

	private final PendingServerCallState call;
	private final BlockingRpcCallback serviceCallback = new BlockingRpcCallback();

	private Thread runningThread = null;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(CallRunner.class);

	public CallRunner(PendingServerCallState call) {
		this.call = call;
		this.call.setExecutor(this);
	}

	public void run() {
		runningThread = Thread.currentThread();
		
		if (call.getController().isCanceled()) {
			return;
		}
		
		try{
			call.getService().callMethod(call.getMethodDesc(),
					call.getController(), call.getRequest(), serviceCallback);
		}catch(Throwable e)
		{
			LOGGER.debug("exec {} meet excetip {}", call.getMethodDesc(), e);
			if ( e.getMessage() == null ){
				call.getController().setFailed( e.toString() );
			}else{
				call.getController().setFailed(e.getMessage());
			}
			serviceCallback.run(null);
		}
		
		
		if (!serviceCallback.isDone()) {
			synchronized (serviceCallback) {
				while (!serviceCallback.isDone()) {
					try {
						serviceCallback.wait();
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		}
//		if (Thread.interrupted()) {
//		}
	}

	public void setRunningThread(Thread runningThread) {
		this.runningThread = runningThread;
	}

	public PendingServerCallState getCall() {
		return call;
	}

	public Thread getRunningThread() {
		return runningThread;
	}

	public BlockingRpcCallback getServiceCallback() {
		return serviceCallback;
	}

}