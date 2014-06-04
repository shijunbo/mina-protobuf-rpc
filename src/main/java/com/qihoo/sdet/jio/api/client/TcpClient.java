package com.qihoo.sdet.jio.api.client;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.UnorderedThreadPoolExecutor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcController;
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
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.ConnectResponse;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService;
import com.qihoo.sdet.jio.api.stream.StreamServiceImpl;
import com.qihoo.sdet.jio.api.stream.StreamWatch;

public class TcpClient<T extends com.google.protobuf.Service> extends
		IoHandlerAdapter implements Bootstrap {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TcpClient.class);
	private PeerInfo clientInfo;
	private PeerInfo serverInfo;
	private RpcServiceRegistry rpcServiceRegistry = new RpcServiceRegistry();
	private RpcServerCallExecutor rpcServerCallExecutor;
	private RpcLogger logger = new CategoryPerServiceLogger();
	private AtomicInteger correlationId = new AtomicInteger(1);
	private T serviceImpl;
	private RpcClientChannel channel = null;
	private Object service;
	private StreamWatch stream = new StreamWatch();
	private IoConnector connector;
	private IoSession session;
	private ExecutorService excutor;
	private TcpConnectionEventListener listenr = new RpcChannelEventNotifier(
			RpcClientWatchdog.getInstance());
	private boolean reconnect = true;

	public TcpClient() {
		this(null);
	}
	
	@SuppressWarnings("unchecked")
	private void rpcService(Object service) throws Exception{
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

		this.service = service;
		rpcServiceRegistry.registerService(StreamService
				.newReflectiveService(new StreamServiceImpl(stream)));

		connector = new NioSocketConnector();
		connector.getFilterChain().addLast("heartbeat", new HeartBeatFilter());
		connector.getFilterChain().addLast("codec",
				new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		connector.setHandler((IoHandler) service);
	}

	public RpcClientChannel initRpcService(Object service) throws Exception {
		rpcService(service);
		String sname = service.getClass().getName();
		if (!Config.getInstance().isClient(sname)) {
			throw new ConfigException("Conf hase no " + sname + " Client");
		}
		serverInfo = new PeerInfo(Config.getInstance().getClientHost(sname),
				Integer.valueOf(Config.getInstance().getClientPort(sname)));
		return peerWith(serverInfo);
	}
	
	public RpcClientChannel initRpcService(Object service, String host, int port) throws Exception {
		rpcService(service);
		serverInfo = new PeerInfo(host, port);
		reconnect = false;
		return peerWith(serverInfo);
	}

	public TcpClient(RpcServerCallExecutor rpcServerCallExecutor) {
		setRpcServerCallExecutor(rpcServerCallExecutor);
	}

	public RpcController getRpcController() {
		return channel.newRpcController();
	}

	public RpcClientChannel peerWith(PeerInfo serverInfo) throws Exception {
		return peerWith(serverInfo.getHostName(), serverInfo.getPort());
	}

	public void peerResponse(ConnectResponse connectResponse) {

		if (connectResponse.hasErrorCode()) {
			session.close(true).awaitUninterruptibly();
			LOGGER.error("TcpServer CONNECT_RESPONSE indicated error "
					+ connectResponse.getErrorCode());
			return;
		}
		if (!connectResponse.hasCorrelationId()) {
			session.close(true).awaitUninterruptibly();
			LOGGER.error("TcpServer CONNECT_RESPONSE missing correlationId.");
			return;
		}

		if (connectResponse.getCorrelationId() != correlationId.get()) {
			session.close(true).awaitUninterruptibly();
			LOGGER.debug("TcpServer CONNECT_RESPONSE correlationId mismatch. TcpClient sent "
					+ correlationId
					+ " received "
					+ connectResponse.getCorrelationId() + " from TcpServer.");
			return;
		}

		RpcClient rpcClient = null;
		serverInfo.setPid(connectResponse.hasServerPID() ? connectResponse
				.getServerPID() : "<NONE>");
		rpcClient = new RpcClient((Bootstrap) service, session, clientInfo,
				serverInfo, stream, reconnect, listenr);
		rpcClient.setCallLogger(getRpcLogger());
		RpcClientHandler rpcClientHandler = completeFilterChain(rpcClient);
		channel = rpcClient;
		rpcClientHandler.notifyOpened();
	}

	public RpcClientChannel peerWith(String host, int port) throws Exception {
		ConnectFuture future = connector.connect(new InetSocketAddress(host,
				port));
		future.awaitUninterruptibly();
		RpcClient rpcClient = null;
		if (future.isConnected()) {
			session = future.getSession();
			InetSocketAddress localAddress = (InetSocketAddress) session
					.getLocalAddress();
			clientInfo = new PeerInfo(localAddress.getHostName(),
					localAddress.getPort());
			rpcClient = new RpcClient((Bootstrap) service, session, clientInfo,
					serverInfo, stream, reconnect, listenr);
			rpcClient.setCallLogger(getRpcLogger());
			RpcClientHandler rpcClientHandler = completeFilterChain(rpcClient);
			channel = rpcClient;
			rpcClientHandler.notifyOpened();
			return channel;

		} else {
			PeerInfo serverInfo = new PeerInfo(host, port, "<NONE>");
			rpcClient = new RpcClient((Bootstrap) service, null, clientInfo,
					serverInfo, null, reconnect, listenr);
			channel = null;
			if (reconnect){
				RpcClientWatchdog.getInstance().connectionLost(rpcClient);
			}else{
				throw new ConnectException("Connect Server[" + host + ":" + port + "] Failure");
			}
		}
		return rpcClient;
	}

	protected RpcClientHandler completeFilterChain(RpcClient rpcClient) {

		RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient,
				listenr);
		rpcClient.getChannel().getFilterChain().addLast(Handler.RPC_CLIENT, rpcClientHandler);

		RpcServer rpcServer = new RpcServer(rpcClient, rpcServiceRegistry,
				rpcServerCallExecutor, logger);
		RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer);
		rpcClient
				.getChannel()
				.getFilterChain()
				.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER,
						rpcServerHandler);
		rpcClient.getChannel().getFilterChain()
				.addAfter("codec", "threadPool", new ExecutorFilter(excutor));
		return rpcClientHandler;
	}

	private void setRpcServerCallExecutor(
			RpcServerCallExecutor rpcServerCallExecutor) {
		this.rpcServerCallExecutor = rpcServerCallExecutor;
		this.excutor = new UnorderedThreadPoolExecutor(Integer.MAX_VALUE);
	}

	private RpcLogger getRpcLogger() {
		return logger;
	}

	public void sessionClosed(RpcClientChannel rpcChannel) {
	}

	public void sessionOpen(RpcClientChannel rpcChannel) {
	}
	
	public static class ConnectException extends Exception
	{
		private static final long serialVersionUID = 1L;
		public ConnectException(String info)
		{
			super(info);
		}
	}
}
