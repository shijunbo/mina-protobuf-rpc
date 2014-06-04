package com.qihoo.sdet.jio.api.logging;

import com.google.protobuf.Message;
import com.qihoo.sdet.jio.api.core.PeerInfo;
import com.qihoo.sdet.jio.api.logging.RpcLogEntry.RpcPayloadInfo;

public interface RpcLogger {
	public void logCall(PeerInfo client, PeerInfo server, String signature,
			Message request, Message response, String errorMessage,
			int correlationId, RpcPayloadInfo reqInfo, RpcPayloadInfo resInfo);
}
