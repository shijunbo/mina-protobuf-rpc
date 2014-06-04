package com.qihoo.sdet.jio.api.client;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.core.PeerInfo;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.listener.RpcChannelEventListener;


public class RpcClientWatchdog implements RpcChannelEventListener {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcClientWatchdog.class);

	private List<RetryState> watchedClients = new ArrayList<RetryState>();

	private Thread thread;
	private WatchdogThread watchdogThread;
	private long retryIntervalMillis = 30000;

	private volatile static RpcClientWatchdog dog = null;

	private RpcClientWatchdog() {
	}

	public static RpcClientWatchdog getInstance() {
		if (dog == null) {
			synchronized (RpcClientWatchdog.class) {
				if (dog == null) {
					dog = new RpcClientWatchdog();
					dog.start();
				}
			}
		}
		return dog;
	}

	public void start() {
		watchdogThread = new WatchdogThread(this);
		thread = new Thread(watchdogThread);
		thread.setDaemon(true);
		thread.start();
	}

	public void stop() {
		if (watchdogThread != null)
			watchdogThread.finish();
		watchdogThread = null;
		if (thread != null)
			thread = null;
	}

	private static class RetryState {
		long lastRetryTime = 0;
		RpcClientChannel rpcClientChannel = null;

		public RetryState(RpcClientChannel clientChannel) {
			rpcClientChannel = clientChannel;
			lastRetryTime = System.currentTimeMillis();
		}
	}

	boolean isRetryableNow(RetryState state) {
		if ( (System.currentTimeMillis() - state.lastRetryTime) >= getRetryIntervalMillis() )
			return true;
		return false;
	}

	List<RetryState> getRetryableStates() {
		List<RetryState> retryList = new ArrayList<RetryState>();
		synchronized (watchedClients) {
			for (RetryState state : watchedClients) {
				if (isRetryableNow(state)) {
					retryList.add(state);
				}
			}
		}
		return retryList;
	}

	void addRetryState(RetryState state) {
		LOGGER.debug("add RetryState state {}", state);
		synchronized (watchedClients) {
			watchedClients.add(state);
		}
	}

	void removeRetryState(RetryState state) {
		synchronized (watchedClients) {
			watchedClients.remove(state);
		}
	}

	void trigger() {
		WatchdogThread wt = watchdogThread;
		if (wt != null) {
			wt.trigger();
		}
	}

	public static class WatchdogThread implements Runnable {

		private boolean stopped = false;
		private RpcClientWatchdog watchdog;
		private Object triggerSyncObject = new Object();

		public WatchdogThread(RpcClientWatchdog watchdog) {
			this.watchdog = watchdog;
		}

		@Override
		public void run() {
			while (!stopped) {
				do {
					List<RetryState> retryableStates = watchdog
							.getRetryableStates();
					if (retryableStates.size() == 0) {
						try {
							synchronized (triggerSyncObject) {
								triggerSyncObject.wait(watchdog
										.getRetryIntervalMillis());
							}
						} catch (InterruptedException e) {

						}
					} else {
						for (RetryState state : retryableStates) {
							doRetry(state);
						}
					}
				} while (!stopped);
			}
		}

		public void finish() {
			this.stopped = true;
			trigger();
		}

		public void trigger() {
			synchronized (triggerSyncObject) {
				triggerSyncObject.notifyAll();
			}
		}

		void doRetry(RetryState state) {

			RpcClientChannel disconnectedClient = state.rpcClientChannel;

			PeerInfo serverInfo = disconnectedClient.getPeerInfo();
		
			try {
				LOGGER.info("Retry connecting servrinfo {} disconnectedClient {}", serverInfo, disconnectedClient);
				watchdog.removeRetryState(state);
				((TcpClient<?>) disconnectedClient
						.getBootstrap()).peerWith(serverInfo);
				
			} catch (Exception e) {
				// leave it in the list
				LOGGER.info("Retry failed " + serverInfo, e);
			}
		}
	}

	public void connectionLost(RpcClientChannel clientChannel) {
		addRetryState(new RetryState(clientChannel));
		trigger();
	}

	public void connectionOpened(RpcClientChannel clientChannel) {
	}

	public void connectionReestablished(RpcClientChannel clientChannel) {
	}

	public void connectionChanged(RpcClientChannel clientChannel) {
	}

	public long getRetryIntervalMillis() {
		return retryIntervalMillis;
	}

	public void setRetryIntervalMillis(long retryIntervalMillis) {
		this.retryIntervalMillis = retryIntervalMillis;
	}

}
