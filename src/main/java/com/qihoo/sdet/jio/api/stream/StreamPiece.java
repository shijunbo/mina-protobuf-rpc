package com.qihoo.sdet.jio.api.stream;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.mina.core.buffer.IoBuffer;

public class StreamPiece {

	private ByteBuffer[] dsts;
	private IoBuffer buf;
	private File file;
	private int offset;

	public StreamPiece(String path, int offset) throws Exception {
		file = new File(path);
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fc = raf.getChannel();
		this.offset = offset;
		dsts = new ByteBuffer[16];
		for (int i = 0; i < dsts.length; i++) {
			dsts[i] = ByteBuffer.allocate(1024);
		}
		fc.position(offset);
		fc.read(dsts, 0, 16);
		buf = IoBuffer.allocate(1024).setAutoExpand(true);
		raf.close();
	}

	public IoBuffer getBuf() {
		for (int index = 0; index < dsts.length; index++) {
			if (dsts[index].position() != 0) {
				dsts[index].flip();
				buf.put(dsts[index]);
			}
		}
		buf.flip();
		return buf;
	}

	public int getOffset() {
		return offset;
	}

	public int getLen() {
		return (int) file.length();
	}

	public String toString() {
		StringBuffer str = new StringBuffer(256);
		str.append("StreamPiece path: ").append(file.getAbsolutePath())
				.append(" size: ").append(file.length()).append(" offset: ")
				.append(offset);
		return str.toString();
	}
}