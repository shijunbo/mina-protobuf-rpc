package com.qihoo.sdet.jio.api.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.core.RpcClient;

public class RpcClientRegistry {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcClientRegistry.class);

	private Map<String, RpcClient> clientNameMap = new ConcurrentHashMap<String, RpcClient>();

	public RpcClientRegistry() {
	}

	public boolean registerRpcClient(RpcClient rpcClient) {
		LOGGER.debug("RpcClient " + rpcClient.getServerInfo()
				+ " register...");
		
		RpcClient existingClient = clientNameMap.get(rpcClient.getServerInfo()
				.getName());
		if (existingClient == null) {
			
			clientNameMap.put(rpcClient.getServerInfo().getName(), rpcClient);
			return true;
		}
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("RpcClient " + rpcClient.getServerInfo()
					+ " is already registered with "
					+ existingClient.getServerInfo());
		return false;
	}

	public void removeRpcClient(RpcClient rpcClient) {
		clientNameMap.remove(rpcClient.getServerInfo().getName());
	}
}
