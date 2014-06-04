package com.qihoo.sdet.jio.api.stream;

import java.io.OutputStream;

public class StreamRecvInfo {
	private final String uuid;
	private final String peer;
	private final String path;
	private final OutputStream os;

	public StreamRecvInfo(String uuid, String peer, String path, OutputStream os) {
		this.uuid = uuid;
		this.os = os;
		this.peer = peer;
		this.path = path;
	}

	public String uuid() {
		return uuid;
	}

	public OutputStream outputStream() {
		return os;
	}

	public String peer() {
		return peer;
	}

	public String path() {
		return path;
	}

	public String toString() {
		StringBuilder str = new StringBuilder(256);
		str.append("StreamRecvInfo uuid: ").append(uuid).append(" path: ")
				.append(path).append(" peer: ").append(peer);
		return str.toString();
	}
}
