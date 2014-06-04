package com.qihoo.sdet.jio.api.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkRequest;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkResponse;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService;

public class StreamHandler extends Thread {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StreamHandler.class);

	private final List<StreamWatch> watchs = new ArrayList<StreamWatch>();
	private final List<StreamSendInfo> sendStreams = new ArrayList<StreamSendInfo>();
	private ReentrantLock lock;
	private Condition condition;
	private final Map<String, StreamWatch> pendingRequestMap = new ConcurrentHashMap<String, StreamWatch>();
	private volatile static StreamHandler handler = null;

	public static StreamHandler getInstance() {
		if (handler == null) {
			synchronized (StreamHandler.class) {
				if (handler == null) {
					handler = new StreamHandler();
					handler.start();
				}
			}
		}
		return handler;
	}

	private StreamHandler() {
		lock = new ReentrantLock();
		condition = lock.newCondition();
	}

	public void register(StreamWatch watch) {
		watchs.add(watch);
	}

	public void registerStream(StreamSendInfo streamInfo) {
		lock.lock();
		try {
			sendStreams.add(streamInfo);
			StreamWatch w = null;
			for (StreamWatch watch : watchs) {
				if (watch.getStream(streamInfo)) {
					w = watch;
					break;
				}
			}
			pendingRequestMap.put(streamInfo.uuid(), w);
			condition.signalAll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void run() {
		while (true) {
			StreamSendInfo streamInfo = null;
			lock.lock();
			try {
				if (sendStreams.size() == 0) {
					try {
						condition.await(5, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
					}
				} else {

					Iterator<StreamSendInfo> it = sendStreams.iterator();
					if (it.hasNext()) {
						streamInfo = it.next();
						it.remove();
					}
				}

			} finally {
				lock.unlock();
			}
			if (streamInfo != null) {
				sendStream(streamInfo);
			}
		}

	}

	private void sendStream(StreamSendInfo streamInfo) {
		try {
			StreamPiece pe = new StreamPiece(streamInfo.path(),
					streamInfo.offset());
			if (streamInfo.offset() == 0)
				streamInfo.setCapacity(pe.getLen());

			StreamWatch w = null;
			for (StreamWatch watch : watchs) {
				if (watch.getStream(streamInfo)) {
					w = watch;
					break;
				}
			}

			if (w != null) {
				RpcClientChannel channel = streamInfo.channel();
				if (channel != null && channel.getChannel().isConnected()) {
					StreamService serverService = StreamService
							.newStub(channel);

					ChunkRequest chunk = ChunkRequest.newBuilder()
							.setOffset(streamInfo.offset())
							.setCapacity(streamInfo.capacity())
							.setUuid(streamInfo.uuid())
							.setAlias(streamInfo.path())
							.setContent(ByteString.copyFrom(pe.getBuf().buf()))
							.build();

					serverService.stream(channel.newRpcController(), chunk,
							new StreamCallback(streamInfo.uuid()));
				} else {
					LOGGER.error("send stream[" + pe + "]'s session invalid...");
					handler(streamInfo.uuid());
					w.error(streamInfo.path(), "");
					return;
				}

				if (LOGGER.isDebugEnabled())
					LOGGER.debug("send stream[" + pe + "] ok");

			} else {
				LOGGER.error("send stream[" + pe + "] error");
				handler(streamInfo.uuid());
			}

		} catch (Exception e) {
			LOGGER.error("send stream[" + streamInfo + "] error");
			handler(streamInfo.uuid());
		}
	}

	public void handler(ChunkResponse responseMessage) {
		StreamSendInfo streamInfo = new StreamSendInfo(
				responseMessage.getUuid(), responseMessage.getPeer());
		StreamWatch w = null;
		for (StreamWatch watch : watchs) {
			if (watch.getStream(streamInfo)) {
				w = watch;
				break;
			}
		}

		if (w == null) {
			LOGGER.error("handler stream[" + streamInfo + "]'s response error");
			pendingRequestMap.remove(responseMessage.getUuid());
			return;
		}

		if (!responseMessage.getOk()) {
			pendingRequestMap.remove(streamInfo.uuid());
			w.error(streamInfo.uuid(), responseMessage.getAlias());
			return;
		}

		if (responseMessage.getEof()) {
			pendingRequestMap.remove(streamInfo.uuid());
			w.success(streamInfo.uuid(), responseMessage.getAlias());
			return;
		}

		streamInfo.setOffset(responseMessage.getOffset());
		registerStream(streamInfo);
	}
	
	public void handler(String uuid) {
		StreamWatch watch = pendingRequestMap.remove(uuid);
		if (watch == null) {
			LOGGER.error("handler stream[{}]'s response error", uuid);
			return;
		}else{
			watch.error(uuid, null);
			return;
		}
	}
}
