package com.qihoo.sdet.jio.api.stream;

import java.lang.ref.WeakReference;

import com.qihoo.sdet.jio.api.core.RpcClientChannel;

public class StreamSendInfo {
	private final String uuid;
	private final String path;
	private int capacity = 0;
	private int offset = 0;
	private WeakReference<RpcClientChannel> channel;

	public StreamSendInfo(String uuid, String path, RpcClientChannel channel) {
		this.uuid = uuid;
		this.path = path;
		this.channel = new WeakReference<RpcClientChannel>(channel);
	}

	public StreamSendInfo(String uuid, String path) {
		this.uuid = uuid;
		this.path = path;
	}

	public String uuid() {
		return uuid;
	}

	public String path() {
		return path;
	}

	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}

	public int capacity() {
		return capacity;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int offset() {
		return offset;
	}

	public void setChannel(RpcClientChannel channel) {
		this.channel = new WeakReference<RpcClientChannel>(channel);
	}

	public RpcClientChannel channel() {
		return (RpcClientChannel) channel.get();
	}

	public String toString() {
		StringBuilder str = new StringBuilder(256);
		str.append("StreamSendInfo uuid: ").append(uuid).append(" path: ")
				.append(path).append(" capacity: ").append(capacity)
				.append(" offset: ").append(offset).append(" channel: ")
				.append(channel.get());
		return str.toString();
	}
}
