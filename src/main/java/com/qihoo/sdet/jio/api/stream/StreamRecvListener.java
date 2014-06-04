package com.qihoo.sdet.jio.api.stream;

public interface StreamRecvListener {
	public void finish(String uuid);

	public void fatal(String uuid);

	public StreamRecvInfo recvStream(String uuid);

	public void addRecvStream(StreamRecvInfo streamInfo);

	public String getRecvDir();

}
