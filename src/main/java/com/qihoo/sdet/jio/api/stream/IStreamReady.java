package com.qihoo.sdet.jio.api.stream;

public interface IStreamReady {
	void ready(String patch, String peer);
	void exceptCaught(Throwable e);
}
