package com.qihoo.sdet.jio.api.listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.qihoo.sdet.jio.api.core.Bootstrap;
import com.qihoo.sdet.jio.api.core.PeerInfo;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.stream.IStreamReady;

public class RpcChannelEventNotifier implements TcpConnectionEventListener {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcChannelEventNotifier.class);

	private Map<String, RpcClientChannel> peerNameMap = new ConcurrentHashMap<String, RpcClientChannel>();

	private RpcChannelEventListener eventListener;

	public RpcChannelEventNotifier() {
	}

	public RpcChannelEventNotifier(RpcChannelEventListener eventListener) {
		this.eventListener = eventListener;
	}

	public void connectionClosed(RpcClientChannel clientChannel) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("connectionClosed from peer {} with local {}", clientChannel.getLocalInfo(), clientChannel.getPeerInfo());
		RpcChannelEventListener l = getEventListener();
		if (l != null && clientChannel.reconnect()) {
			l.connectionLost(clientChannel);
		}
		peerNameMap.put(clientChannel.getPeerInfo().getName(),
				new DetachedRpcClientChannel(clientChannel.getPeerInfo(),
						clientChannel.getBootstrap()));
		clientChannel.getBootstrap().sessionClosed(clientChannel);
	}

	public void connectionOpened(RpcClientChannel clientChannel) {
		RpcChannelEventListener l = getEventListener();
		PeerInfo peerInfo = clientChannel.getPeerInfo();
		RpcClientChannel existingClient = peerNameMap.get(peerInfo.getName());
		if (existingClient == null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("connectionOpened from peer {} with local {}", clientChannel.getLocalInfo(), clientChannel.getPeerInfo());
			if (l != null) {
				l.connectionOpened(clientChannel);
			}
		} else {
			PeerInfo existingPeerInfo = existingClient.getPeerInfo();
			if (!existingPeerInfo.getPid().equals(
					clientChannel.getPeerInfo().getPid())) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("connectionChanged from " + existingPeerInfo
							+ " to " + peerInfo);
				if (l != null) {
					l.connectionChanged(clientChannel);
				}
			} else {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("connectionReestablished from peer {} with local {}", clientChannel.getLocalInfo(), clientChannel.getPeerInfo());
				if (l != null) {
					l.connectionOpened(clientChannel);
				}
			}
		}

		clientChannel.getBootstrap().sessionOpen(clientChannel);
	}

	public RpcChannelEventListener getEventListener() {
		return eventListener;
	}

	public void setEventListener(RpcChannelEventListener eventListener) {
		this.eventListener = eventListener;
	}

	private static class DetachedRpcClientChannel implements RpcClientChannel {

		private PeerInfo peerInfo;
		private Bootstrap client;

		public DetachedRpcClientChannel(PeerInfo peerInfo, Bootstrap client) {
			this.peerInfo = peerInfo;
			this.client = client;
		}

		public Message callBlockingMethod(MethodDescriptor method,
				RpcController controller, Message request,
				Message responsePrototype) throws ServiceException {
			throw new IllegalStateException(
					"method not supported on detached RpcClientChannel.");
		}

		public void callMethod(MethodDescriptor method,
				RpcController controller, Message request,
				Message responsePrototype, RpcCallback<Message> done) {
			throw new IllegalStateException(
					"method not supported on detached RpcClientChannel.");
		}

		public PeerInfo getPeerInfo() {
			return this.peerInfo;
		}
		
		public PeerInfo getLocalInfo() {
			return null;
		}

		public RpcController newRpcController() {
			throw new IllegalStateException(
					"method not supported on detached RpcClientChannel.");
		}

		public void close() {
			throw new IllegalStateException(
					"method not supported on detached RpcClientChannel.");
		}

		@Override
		public Bootstrap getBootstrap() {
			return client;
		}

		@Override
		public IoSession getChannel() {
			return null;
		}

		@Override
		public IoSession getSession() {
			return null;
		}

		@Override
		public String synStream(String path) {
			return null;
		}

		@Override
		public void stream(String path, IStreamReady ready) {
		}

		@Override
		public boolean reconnect() {
			return false;
		}

	}
}
