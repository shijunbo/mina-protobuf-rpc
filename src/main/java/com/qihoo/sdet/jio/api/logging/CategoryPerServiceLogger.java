package com.qihoo.sdet.jio.api.logging;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.qihoo.sdet.jio.api.core.PeerInfo;
import com.qihoo.sdet.jio.api.logging.RpcLogEntry.RpcCall;
import com.qihoo.sdet.jio.api.logging.RpcLogEntry.RpcPayloadInfo;

public class CategoryPerServiceLogger implements RpcLogger {

	private boolean logRequestProto = true;
	private boolean logResponseProto = true;

	public void logCall(PeerInfo client, PeerInfo server, String signature,
			Message request, Message response, String errorMessage,
			int correlationId, RpcPayloadInfo reqInfo, RpcPayloadInfo resInfo) {

		if (resInfo == null) {
			return;
		}

		int duration = (int) (resInfo.getTs() - reqInfo.getTs());

		RpcCall.Builder rpcCall = RpcCall.newBuilder().setCorId(correlationId)
				.setDuration(duration).setClient(client.toString())
				.setServer(server.toString()).setSignature(signature);
		if (errorMessage != null) {
			rpcCall.setError(errorMessage);
		}
		rpcCall.setRequest(reqInfo);
		rpcCall.setResponse(resInfo);

		String summaryCategoryName = signature + ".info";
		Logger log = LoggerFactory.getLogger(summaryCategoryName);
		String summaryText = null;
		if (log.isDebugEnabled()) {
			summaryText = TextFormat.printToString(rpcCall.build());
		}

		String requestCategoryName = signature + ".data.request";
		Logger reqlog = LoggerFactory.getLogger(requestCategoryName);
		String requestText = null;
		if (isLogRequestProto() && request != null) {
			if (reqlog.isDebugEnabled() && !isBigMessage(request)) {
				requestText = TextFormat.printToString(request);
			}
		}

		String responseCategoryName = signature + ".data.response";
		Logger reslog = LoggerFactory.getLogger(responseCategoryName);
		String responseText = null;
		if (isLogResponseProto() && response != null) {
			if (reslog.isDebugEnabled() && !isBigMessage(response)) {
				responseText = TextFormat.printToString(response);
			}
		}

		if (summaryText != null) {
			synchronized (log) {
				log.info(summaryText);
			}
		}
		if (requestText != null) {
			synchronized (reqlog) {
				reqlog.info(requestText);
			}
		}
		if (responseText != null) {
			synchronized (reslog) {
				reslog.info(responseText);
			}
		}
	}

	private boolean isBigMessage(Message message) {
		Iterator<Map.Entry<FieldDescriptor, Object>> it = message
				.getAllFields().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<FieldDescriptor, Object> entry = it.next();
			FieldDescriptor fd = entry.getKey();
			if (fd.getType() == FieldDescriptor.Type.BYTES) {
				if (((ByteString) entry.getValue()).size() >= 1048576) { // 1MB
					return true;
				}
			}
		}

		return false;
	}

	public boolean isLogRequestProto() {
		return logRequestProto;
	}

	public void setLogRequestProto(boolean logRequestProto) {
		this.logRequestProto = logRequestProto;
	}

	public boolean isLogResponseProto() {
		return logResponseProto;
	}

	public void setLogResponseProto(boolean logResponseProto) {
		this.logResponseProto = logResponseProto;
	}

}
