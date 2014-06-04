package com.qihoo.sdet.jio.api.handler;

import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.core.RpcClient;
import com.qihoo.sdet.jio.api.listener.TcpConnectionEventListener;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.WirePayload;

public class RpcClientHandler extends IoFilterAdapter {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcClientHandler.class);

	private RpcClient rpcClient;
	private TcpConnectionEventListener eventListener;

	public RpcClientHandler(RpcClient rpcClient,
			TcpConnectionEventListener eventListener) {
		if (rpcClient == null) {
			throw new IllegalArgumentException("rpcClient");
		}
		if (eventListener == null) {
			throw new IllegalArgumentException("eventListener");
		}
		this.eventListener = eventListener;
		this.rpcClient = rpcClient;
	}

	public void messageReceived(NextFilter nextFilter, IoSession session,
			Object message) throws Exception {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Processing a MESSAGE_RECEIVED for session"
					+ session.getId());
		if (message instanceof WirePayload) {
			WirePayload payload = (WirePayload) message;
			if (payload.hasRpcResponse()) {
				rpcClient.response(payload.getRpcResponse());
				return;
			} else if (payload.hasRpcError()) {
				rpcClient.error(payload.getRpcError());
				return;
			}
		}
		nextFilter.messageReceived(session, message);
	}

	public void sessionClosed(NextFilter nextFilter, IoSession session)
			throws Exception {
		nextFilter.sessionClosed(session);
		LOGGER.error("sessionClosed {} {}", session, rpcClient);
		rpcClient.handleClosure();
		notifyClosed();
	}

	public void exceptionCaught(NextFilter nextFilter, IoSession session,
			Throwable cause) throws Exception {
		nextFilter.exceptionCaught(session, cause);
		rpcClient.close();
		LOGGER.error("Session {} {} Exception caught during RPC operation: ",
				session, rpcClient, cause);
	}

	public void notifyClosed() {
		eventListener.connectionClosed(rpcClient);
	}

	public void notifyOpened() {
		eventListener.connectionOpened(rpcClient);
	}

	public RpcClient getRpcClient() {
		return rpcClient;
	}
	
	public void sessionIdle(NextFilter nextFilter, IoSession session, IdleStatus status) throws Exception {
		if( status == IdleStatus.READER_IDLE ){
			LOGGER.warn("Session {} {} read idle than 20 seconds",
					session, rpcClient);
			rpcClient.close();
		}
    }
}
