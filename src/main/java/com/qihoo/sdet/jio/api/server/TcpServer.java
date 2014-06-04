package com.qihoo.sdet.jio.api.server;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.apache.mina.core.filterchain.IoFilterChain;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.UnorderedThreadPoolExecutor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import com.qihoo.sdet.jio.api.core.Bootstrap;
import com.qihoo.sdet.jio.api.core.Config;
import com.qihoo.sdet.jio.api.core.ConfigException;
import com.qihoo.sdet.jio.api.core.PeerInfo;
import com.qihoo.sdet.jio.api.core.RpcClient;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.core.RpcServer;
import com.qihoo.sdet.jio.api.core.RpcServiceRegistry;
import com.qihoo.sdet.jio.api.execute.RpcServerCallExecutor;
import com.qihoo.sdet.jio.api.handler.Handler;
import com.qihoo.sdet.jio.api.handler.RpcClientHandler;
import com.qihoo.sdet.jio.api.handler.RpcServerHandler;
import com.qihoo.sdet.jio.api.heartbeat.HeartBeatFilter;
import com.qihoo.sdet.jio.api.listener.RpcChannelEventNotifier;
import com.qihoo.sdet.jio.api.listener.TcpConnectionEventListener;
import com.qihoo.sdet.jio.api.logging.CategoryPerServiceLogger;
import com.qihoo.sdet.jio.api.logging.RpcLogger;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService;
import com.qihoo.sdet.jio.api.stream.StreamServiceImpl;
import com.qihoo.sdet.jio.api.stream.StreamWatch;

public class TcpServer<T extends com.google.protobuf.Service> extends
		IoHandlerAdapter implements Bootstrap {

	private T serviceImpl;
	private final RpcServiceRegistry rpcServiceRegistry = new RpcServiceRegistry();
	private final RpcClientRegistry rpcClientRegistry = new RpcClientRegistry();
	private RpcServerCallExecutor rpcServerCallExecutor = null;
	private final TcpConnectionEventListener eventListener = new RpcChannelEventNotifier();
	private final RpcLogger logger = new CategoryPerServiceLogger();
	private StreamWatch stream = new StreamWatch();
	private Object service;
	private final ExecutorService excutor = new UnorderedThreadPoolExecutor(Integer.MAX_VALUE);

	@SuppressWarnings("unchecked")
	public void initRpcService(Object service) throws Exception {
		if (getClass().getGenericSuperclass() instanceof ParameterizedType) {
			ParameterizedType p = (ParameterizedType) getClass()
					.getGenericSuperclass();
			Method a = ((Class<?>) p.getActualTypeArguments()[0]).getMethod(
					"newReflectiveService", (Class<?>) getClass()
							.getInterfaces()[0]);
			serviceImpl = (T) a.invoke(service.getClass(), service);
			if (serviceImpl != null) {
				rpcServiceRegistry.registerService(serviceImpl);
			}
		}

		String sname = service.getClass().getName();
		if (!Config.getInstance().isServer(sname)) {
			throw new ConfigException("Conf hase no " + sname + " Server");
		}

		this.service = service;
		rpcServiceRegistry.registerService(StreamService
				.newReflectiveService(new StreamServiceImpl(stream)));
		
		InetSocketAddress address = new InetSocketAddress( Integer.valueOf(Config.getInstance().getServerPort(sname)) );
		IoAcceptor acceptor = new NioSocketAcceptor();
		acceptor.getFilterChain().addLast("heartbeat", new HeartBeatFilter());
		acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		acceptor.setHandler((IoHandler) service);
		acceptor.bind(address);
	}

	public void sessionClosed(RpcClientChannel rpcChannel) {
	}

	public void sessionOpen(RpcClientChannel rpcChannel) {
	}
	
	@Override
	public void sessionOpened(IoSession session) {
		InetSocketAddress localAddress = (InetSocketAddress) session
				.getLocalAddress();
		PeerInfo serverInfo = new PeerInfo(localAddress.getHostName(),
				localAddress.getPort());
		InetSocketAddress remoteAddress = (InetSocketAddress) session
				.getRemoteAddress();
		PeerInfo clientInfo = new PeerInfo(remoteAddress.getHostName(),
				remoteAddress.getPort());
		RpcClient rpcClient = new RpcClient((Bootstrap)service,
				session, serverInfo, clientInfo, stream,
				false, eventListener);
		rpcClient.setCallLogger(logger);
		rpcClientRegistry.registerRpcClient(rpcClient);
		RpcClientHandler rpcClientHandler = completeFilterChain(rpcClient);
		rpcClientHandler.notifyOpened();
	}
	
	private RpcClientHandler completeFilterChain(RpcClient rpcClient) {
		IoFilterChain p = rpcClient.getChannel().getFilterChain();

		RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient,
				eventListener);
		p.addLast(Handler.RPC_CLIENT, rpcClientHandler);

		RpcServer rpcServer = new RpcServer(rpcClient, rpcServiceRegistry,
				rpcServerCallExecutor, logger);
		RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer);
		rpcServerHandler.setRpcClientRegistry(rpcClientRegistry);
		p.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER, rpcServerHandler);
		p.addAfter("codec", "threadPool", new ExecutorFilter(excutor));
		return rpcClientHandler;
	}
}
