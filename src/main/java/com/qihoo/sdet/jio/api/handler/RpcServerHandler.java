package com.qihoo.sdet.jio.api.handler;

import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.core.RpcServer;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.WirePayload;
import com.qihoo.sdet.jio.api.server.RpcClientRegistry;

public class RpcServerHandler extends IoFilterAdapter {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcServerHandler.class);

	private RpcServer rpcServer;
	private RpcClientRegistry rpcClientRegistry;

	public RpcServerHandler(RpcServer rpcServer) {
		if (rpcServer == null) {
			throw new IllegalArgumentException("rpcServer");
		}
		this.rpcServer = rpcServer;
	}

	public void messageReceived(NextFilter nextFilter, IoSession session,
			Object message) throws Exception {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Processing a MESSAGE_RECEIVED for session"
					+ session.getId());
		if (message instanceof WirePayload) {
			WirePayload payload = (WirePayload) message;
			if (payload.hasRpcRequest()) {
				rpcServer.request(payload.getRpcRequest());
			} else if (payload.hasRpcCancel()) {
				rpcServer.cancel(payload.getRpcCancel());
			}
			return;
		}
		nextFilter.messageReceived(session, message);
	}

	public void exceptionCaught(NextFilter nextFilter, IoSession session,
			Throwable cause) throws Exception {
		if (rpcClientRegistry != null)
			rpcClientRegistry.removeRpcClient(rpcServer.getRcpClient());
		rpcServer.handleClosure();
		LOGGER.error("Session {} {} Exception caught during RPC operation: ",
				session, rpcServer, cause);
	}

	public void sessionClosed(NextFilter nextFilter, IoSession session)
			throws Exception {
		nextFilter.sessionClosed(session);
		if (rpcClientRegistry != null)
			rpcClientRegistry.removeRpcClient(rpcServer.getRcpClient());
		rpcServer.handleClosure();
	}

	public RpcClientRegistry getRpcClientRegistry() {
		return rpcClientRegistry;
	}

	public void setRpcClientRegistry(RpcClientRegistry rpcClientRegistry) {
		this.rpcClientRegistry = rpcClientRegistry;
	}

	public RpcServer getRpcServer() {
		return rpcServer;
	}

}
