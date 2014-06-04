package com.qihoo.sdet.jio.api.stream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkRequest;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.ChunkResponse;
import com.qihoo.sdet.jio.api.protocol.StreamProtocol.StreamService;
import com.qihoo.sdet.jio.api.util.Utils;

public class StreamServiceImpl implements StreamService.Interface {

	private final StreamRecvListener listener;
	private final ReentrantLock lock = new ReentrantLock();
	private static final Logger LOGGER = LoggerFactory
			.getLogger(StreamServiceImpl.class);
	
	@Override
	public void stream(RpcController controller, ChunkRequest request,
			RpcCallback<ChunkResponse> done) {
		
		StreamRecvInfo streamInfo = listener.recvStream(request.getUuid());

		if (streamInfo == null) {
			String path = null;
			if (request.hasAlias()) {
				lock.lock();
				try{
					path = listener.getRecvDir() + File.separator + Utils.getName(request.getAlias());
					if ( new File(path).exists() ){
						path = listener.getRecvDir() + File.separator
								+ request.getUuid() + "_" + Utils.getName(request.getAlias());
					}
				}finally{
					lock.unlock();
				}
			} else {
				path = listener.getRecvDir() + File.separator
						+ request.getUuid() + "_" + Utils.getNowTime();
			}

			FileOutputStream os = null;
			try {
				os = new FileOutputStream(new File(path));
				streamInfo = new StreamRecvInfo(request.getUuid(),
						request.getAlias(), path, os);
				listener.addRecvStream(streamInfo);
			} catch (FileNotFoundException e) {
				ChunkResponse response = ChunkResponse.newBuilder()
						.setUuid(request.getUuid())
						.setOffset(request.getOffset()).setOk(false)
						.setPeer(request.getAlias()).setEof(false).build();
				done.run(response);
				LOGGER.error("File {} not Found", path);
				e.printStackTrace();
				listener.fatal(request.getUuid());
				return;
			}
		}

		OutputStream os = streamInfo.outputStream();
		byte[] bs = request.getContent().toByteArray();
		try {
			os.write(bs);
		} catch (IOException e) {
			ChunkResponse response = ChunkResponse.newBuilder()
					.setUuid(request.getUuid()).setOffset(request.getOffset())
					.setPeer(streamInfo.peer()).setOk(false).setEof(false)
					.build();
			done.run(response);
			e.printStackTrace();
			LOGGER.error("File {} write IOExcetion", streamInfo);
			listener.fatal(request.getUuid());
			try {
				os.close();
			} catch (Exception ee) {
			}
			return;
		}

		boolean eof = request.getOffset() + bs.length >= request.getCapacity();
		ChunkResponse response = ChunkResponse.newBuilder()
				.setUuid(request.getUuid())
				.setOffset(request.getOffset() + bs.length).setEof(eof)
				.setAlias(streamInfo.path()).setOk(true)
				.setPeer(streamInfo.peer()).build();

		done.run(response);
		if (eof){
			try {
				os.close();
			} catch (Exception e) {
			}
			listener.finish(request.getUuid());
		}
	}

	public StreamServiceImpl(StreamRecvListener listener) {
		this.listener = listener;
	}
}
