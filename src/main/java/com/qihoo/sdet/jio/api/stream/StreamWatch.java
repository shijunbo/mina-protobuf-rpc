package com.qihoo.sdet.jio.api.stream;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.qihoo.sdet.jio.api.core.Config;
import com.qihoo.sdet.jio.api.core.ConfigException;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkRequest;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkResponse;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService.BlockingInterface;
import com.qihoo.sdet.jio.api.util.Utils;

public class StreamWatch implements StreamRecvListener {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StreamWatch.class);

	private final Map<String, StreamSendInfo> pendingSendStreamMap = new ConcurrentHashMap<String, StreamSendInfo>();
	private final Map<String, StreamRecvInfo> pendingRecvStreamMap = new ConcurrentHashMap<String, StreamRecvInfo>();
	private final Map<String, IStreamReady> pendingSendStreamReadyMap = new ConcurrentHashMap<String, IStreamReady>();
	private String recvDir;

	final ExecutorService service = Executors.newFixedThreadPool(1);

	static public class StreamCallable implements Callable<String> {
		private StreamSendInfo _streamInfo;
		private WeakReference<RpcClientChannel> _channel;

		public StreamCallable(StreamSendInfo streamInfo,
				RpcClientChannel rpcChannel) {
			_streamInfo = streamInfo;
			
			_channel = new WeakReference<RpcClientChannel>(rpcChannel);
		}

		public ChunkResponse stream() throws Exception {
			StreamPiece pe = new StreamPiece(_streamInfo.path(),
					_streamInfo.offset());
			RpcClientChannel channel = _channel.get();
			if ( channel == null || !channel.getSession().isConnected() ){
				throw new Exception("channel close?");
			}
			
			BlockingInterface serverService = StreamService.newBlockingStub(channel);

			ChunkRequest chunk = ChunkRequest.newBuilder()
					.setOffset(_streamInfo.offset())
					.setCapacity(_streamInfo.capacity())
					.setUuid(_streamInfo.uuid()).setAlias(_streamInfo.path())
					.setContent(ByteString.copyFrom(pe.getBuf().buf())).build();
			return serverService.stream(channel.newRpcController(), chunk);
		}

		public String call() throws Exception {
			_streamInfo.setCapacity(new StreamPiece(_streamInfo.path(), _streamInfo.offset()).getLen());
			
			ChunkResponse responseMessage = stream();
			while (responseMessage.getOk() && !responseMessage.getEof()) {
				_streamInfo.setOffset(responseMessage.getOffset());
				responseMessage = stream();
				if ( responseMessage == null ){
					throw new Exception("channel close??");
				}
			}
			if (!responseMessage.getOk()) {
				return null;
			}

			if (responseMessage.getEof()) {
				return responseMessage.getAlias();
			}
			return null;
		}
	}

	public StreamWatch() {
		init();
		StreamHandler.getInstance().register(this);
	}

	public String getRecvDir() {
		return recvDir;
	}

	private void init() {
		if (Config.getInstance().getFirstNode("recdir") != null
				&& Config.getInstance().getFirstNode("recdir").getAttribute()
						.get("value") != null) {
			if ( Utils.isAndroid() ){
				recvDir = File.separator
						+ "sdcard"
						+ File.separator
						+ Config.getInstance().getFirstNode("recdir")
						.getAttribute().get("value");
			}else{
				recvDir = System.getProperty("user.dir")
					+ File.separator
					+ Config.getInstance().getFirstNode("recdir")
							.getAttribute().get("value");
			}
		} else {
			if ( Utils.isAndroid() ){
				recvDir = File.separator
						+ "sdcard"
						+ File.separator
						+ "jiorecfiles";
			}else{
				recvDir = System.getProperty("user.dir") + File.separator
						+ "jiorecfiles";
			}
		}
		try {
			Utils.mkdirs(recvDir);
		} catch (Exception e) {
			throw new ConfigException("Create Dir " + recvDir + "Failure");
		}
	}

	public boolean getStream(StreamSendInfo streamInfo) {
		if (pendingSendStreamMap.containsKey(streamInfo.uuid())) {
			StreamSendInfo streaminfo = pendingSendStreamMap.get(streamInfo
					.uuid());
			if (streaminfo.path().equalsIgnoreCase(streamInfo.path())) {
				streamInfo.setCapacity(streaminfo.capacity());
				streamInfo.setChannel(streaminfo.channel());
				return true;
			}
		}
		return false;
	}

	public void sendStream(String path, RpcClientChannel channel, IStreamReady ready) {
		String uuid = UUID.randomUUID().toString();
		if (pendingSendStreamMap.containsKey(uuid)) {
			LOGGER.error("Sending stream[" + uuid + "] already exist");
			ready.exceptCaught(new StreamException("Sending stream[" + path + "] already exist"));
			return;
		}
		StreamSendInfo streamInfo = new StreamSendInfo(uuid, path, channel);
		pendingSendStreamMap.put(uuid, streamInfo);
		pendingSendStreamReadyMap.put(uuid, ready);
		StreamHandler.getInstance().registerStream(streamInfo);
	}

	public String synSendStream(String path, RpcClientChannel channel) throws ExecutionException {
		StreamSendInfo streamInfo = new StreamSendInfo(UUID.randomUUID().toString(), path, channel);
		Future<String> taskFuture = service.submit(new StreamCallable(streamInfo, channel));
		try
		{
			return taskFuture.get(5000, TimeUnit.SECONDS);
		}
		catch (InterruptedException e)
		{
			LOGGER.error("Send stream {} with channel {} InterruptedException {} ", path, channel, e);
		}
		catch (ExecutionException e)
		{
			LOGGER.error("Send stream {} with channel {} MeetException {} ", path, channel, e);
			throw e;
		}
		catch (TimeoutException e)
		{
			LOGGER.error("Send stream {} with channel {} timeout {} ", path, channel, e, 500);
			taskFuture.cancel(true);
		}
		return null;
	}

	public StreamRecvInfo recvStream(String uuid) {
		if (!pendingRecvStreamMap.containsKey(uuid)) {
			LOGGER.warn("recv stream[" + uuid + "] not exist");
			return null;
		}
		return pendingRecvStreamMap.get(uuid);
	}

	public void addRecvStream(StreamRecvInfo streamInfo) {
		pendingRecvStreamMap.put(streamInfo.uuid(), streamInfo);
	}

	public void success(String uuid, String peerPath) {
		IStreamReady ready = pendingSendStreamReadyMap.remove(uuid);
		StreamSendInfo ss = pendingSendStreamMap.remove(uuid);
		if ( ss != null ){
			ready.ready(ss.path(), peerPath);
		}else{
			ready.exceptCaught(new StreamException("Sending stream[" + uuid + "]'s info not exist"));
		}
	}

	public void error(String uuid, String peerPath) {
		IStreamReady ready = pendingSendStreamReadyMap.remove(uuid);
		pendingSendStreamMap.remove(uuid);
		ready.exceptCaught(new StreamException("Send stream[" + uuid + "] failure") );
	}

	public void finish(String uuid) {
		if (pendingRecvStreamMap.containsKey(uuid)) {
			LOGGER.info("recv stream[" + pendingRecvStreamMap.remove(uuid)
					+ "] finish");
			return;
		}
		LOGGER.warn("recv stream[" + uuid + "] not exist");
	}

	public void fatal(String uuid) {
		if (pendingRecvStreamMap.containsKey(uuid)) {
			LOGGER.info("recv stream[" + pendingRecvStreamMap.remove(uuid)
					+ "] meet fatal error");
			return;
		}
		LOGGER.warn("recv stream[" + uuid + "] not exist");
	}
	
	@Override
	protected void finalize() throws Throwable
	{
		if ( service != null ){
			service.shutdown();
		}
	}
	
	
	public static class StreamException extends Exception
	{
		private static final long serialVersionUID = 1L;
		public StreamException(String info)
		{
			super(info);
		}
	}
}
