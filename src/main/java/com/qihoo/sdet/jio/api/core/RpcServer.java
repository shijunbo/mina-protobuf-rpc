package com.qihoo.sdet.jio.api.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;
import com.qihoo.sdet.jio.api.execute.CallRunner;
import com.qihoo.sdet.jio.api.execute.PendingServerCallState;
import com.qihoo.sdet.jio.api.execute.RpcServerCallExecutor;
import com.qihoo.sdet.jio.api.execute.RpcServerExecutorCallback;
import com.qihoo.sdet.jio.api.execute.ServerRpcController;
import com.qihoo.sdet.jio.api.logging.RpcLogEntry.RpcPayloadInfo;
import com.qihoo.sdet.jio.api.logging.RpcLogger;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcCancel;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcError;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcRequest;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcResponse;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.WirePayload;

public class RpcServer implements RpcServerExecutorCallback {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RpcServer.class);

	private final Map<Integer, PendingServerCallState> pendingServerCallMap = new ConcurrentHashMap<Integer, PendingServerCallState>();

	private final RpcClient rpcClient;
	private final RpcServiceRegistry rpcServiceRegistry;
	private final RpcLogger logger;

	public RpcServer(RpcClient rcpClient,
			RpcServiceRegistry rpcServiceRegistry,
			RpcServerCallExecutor callExecutor, RpcLogger logger) {
		this.rpcClient = rcpClient;
		this.rpcServiceRegistry = rpcServiceRegistry;
		this.logger = logger;
	}
	
	public void request(RpcRequest rpcRequest) {
		long startTS = System.currentTimeMillis();
		int correlationId = rpcRequest.getCorrelationId();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Received ["+rpcRequest.getCorrelationId()+"]RpcRequest. signature {}:{}", rpcRequest.getServiceIdentifier(), rpcRequest.getMethodIdentifier());

		if (pendingServerCallMap.containsKey(correlationId)) {
			throw new IllegalStateException("correlationId " + correlationId
					+ " already registered as PendingServerCall.");
		}

		Service service = rpcServiceRegistry.resolveService(rpcRequest
				.getServiceIdentifier());
		if (service == null) {
			String errorMessage = "Unknown Service";
			RpcError rpcError = RpcError.newBuilder()
					.setCorrelationId(correlationId)
					.setErrorMessage(errorMessage).build();
			WirePayload payload = WirePayload.newBuilder()
					.setRpcError(rpcError).build();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Sending [" + rpcError.getCorrelationId()
						+ "]RpcError.");
			rpcClient.getChannel().write(payload);

			doErrorLog(correlationId, "Unknown", rpcRequest, rpcError,
					errorMessage);
			return;
		}
		MethodDescriptor methodDesc = service.getDescriptorForType()
				.findMethodByName(rpcRequest.getMethodIdentifier());
		if (methodDesc == null) {
			String errorMessage = "Unknown Method";
			RpcError rpcError = RpcError.newBuilder()
					.setCorrelationId(correlationId)
					.setErrorMessage(errorMessage).build();
			WirePayload payload = WirePayload.newBuilder()
					.setRpcError(rpcError).build();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Sending [" + rpcError.getCorrelationId()
						+ "]RpcError.");
			rpcClient.getChannel().write(payload);

			doErrorLog(correlationId, "Unknown", rpcRequest, rpcError,
					errorMessage);
			return;
		}
		Message requestPrototype = service.getRequestPrototype(methodDesc);
		Message request = null;
		try {
			request = requestPrototype.newBuilderForType()
					.mergeFrom(rpcRequest.getRequestBytes()).build();

		} catch (InvalidProtocolBufferException e) {
			String errorMessage = "Invalid Request Protobuf";

			RpcError rpcError = RpcError.newBuilder()
					.setCorrelationId(correlationId)
					.setErrorMessage(errorMessage).build();
			WirePayload payload = WirePayload.newBuilder()
					.setRpcError(rpcError).build();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Sending [" + rpcError.getCorrelationId()
						+ "]RpcError.");
			rpcClient.getChannel().write(payload);

			doErrorLog(correlationId, methodDesc.getFullName(), rpcRequest,
					rpcError, errorMessage);
			return;
		}
		ServerRpcController controller = new ServerRpcController(rpcClient,
				correlationId);

		PendingServerCallState state = new PendingServerCallState(this,
				service, controller, methodDesc, request, startTS);
		pendingServerCallMap.put(correlationId, state);
		
		CallRunner runner = new CallRunner(state);
		runner.run();
		
		if (controller.isCanceled()) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Received [" + rpcRequest.getCorrelationId()
						+ "]RpcRequest is Canceled.");
			if (controller.getAndSetCancelCallbackNotified()) {
				RpcCallback<Object> cancelCallback = controller
						.getCancelNotifyCallback();
				if (cancelCallback != null) {
					cancelCallback.run(null);
				}
			}
		} else {
			
			if (!runner.getServiceCallback().isDone())
				LOGGER.warn("RpcCallRunner did not finish RpcCall afterExecute. RpcCallRunner expected to complete calls, not offload them.");
			runner.getCall()
					.getExecutorCallback()
					.onFinish(
							runner.getCall().getController()
									.getCorrelationId(),
							runner.getServiceCallback().getMessage());
		}
	}

	public void cancel(RpcCancel rpcCancel) {
		int correlationId = rpcCancel.getCorrelationId();

		PendingServerCallState state = pendingServerCallMap
				.remove(correlationId);
		if (state != null) {
			// we only issue one cancel to the Executor
			state.getController().startCancel();
			if ( state.getExecutor().getRunningThread().isAlive() ){
				state.getExecutor().getRunningThread().interrupt();
			}
			
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Received [" + rpcCancel.getCorrelationId()
						+ "]RpcCancel.");
			doLog(state, rpcCancel, "Cancelled");
		}
	}

	public String toString() {
		return "RpcServer[" + getRcpClient() + "]";
	}

	public void onFinish(int correlationId, Message message) {
		PendingServerCallState state = pendingServerCallMap
				.remove(correlationId);
		if (state != null) {
			// finished successfully, or failed - respond
			if (message != null) {
				RpcResponse rpcResponse = RpcResponse.newBuilder()
						.setCorrelationId(correlationId)
						.setResponseBytes(message.toByteString()).build();
				WirePayload payload = WirePayload.newBuilder()
						.setRpcResponse(rpcResponse).build();
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Sending [" + rpcResponse.getCorrelationId()
							+ "]RpcResponse.");
				rpcClient.getChannel().write(payload);

				doLog(state, message, null);
			} else {
				String errorMessage = state.getController().getFailed();
				RpcError rpcError = RpcError.newBuilder()
						.setCorrelationId(correlationId)
						.setErrorMessage(errorMessage).build();
				WirePayload payload = WirePayload.newBuilder()
						.setRpcError(rpcError).build();
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Sending [" + rpcError.getCorrelationId()
							+ "]RpcError.");
				rpcClient.getChannel().write(payload);

				doLog(state, rpcError, errorMessage);
			}
		} else {
			// RPC call canceled by client - we don't respond
		}
	}

	public void handleClosure() {
		List<Integer> pendingCallIds = new ArrayList<Integer>();
		pendingCallIds.addAll(pendingServerCallMap.keySet());
		do {
			for (Integer correlationId : pendingCallIds) {
				PendingServerCallState state = pendingServerCallMap
						.remove(correlationId);
				if (state != null) {
					state.getController().startCancel();
					if ( state.getExecutor().getRunningThread().isAlive() ){
						state.getExecutor().getRunningThread().interrupt();
					}
					
					RpcCancel rpcCancel = RpcCancel.newBuilder()
							.setCorrelationId(correlationId).build();
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Cancel on close ["
								+ rpcCancel.getCorrelationId() + "]RpcCancel.");
					doLog(state, rpcCancel, "Cancelled on Close");
				}
			}
		} while (pendingServerCallMap.size() > 0);
	}

	protected void doErrorLog(int correlationId, String signature,
			Message request, Message response, String errorMessage) {
		if (logger != null) {
			RpcPayloadInfo reqInfo = RpcPayloadInfo.newBuilder()
					.setSize(request.getSerializedSize())
					.setTs(System.currentTimeMillis()).build();
			RpcPayloadInfo resInfo = RpcPayloadInfo.newBuilder()
					.setSize(response.getSerializedSize())
					.setTs(System.currentTimeMillis()).build();
			logger.logCall(rpcClient.getClientInfo(),
					rpcClient.getServerInfo(), signature, request, response,
					errorMessage, correlationId, reqInfo, resInfo);
		}
	}

	protected void doLog(PendingServerCallState state, Message response,
			String errorMessage) {
		if (logger != null) {
			RpcPayloadInfo reqInfo = RpcPayloadInfo.newBuilder()
					.setSize(state.getRequest().getSerializedSize())
					.setTs(state.getStartTS()).build();
			RpcPayloadInfo resInfo = RpcPayloadInfo.newBuilder()
					.setSize(response.getSerializedSize())
					.setTs(System.currentTimeMillis()).build();
			logger.logCall(rpcClient.getClientInfo(),
					rpcClient.getServerInfo(), state.getMethodDesc()
							.getFullName(), state.getRequest(), response,
					errorMessage, state.getController().getCorrelationId(),
					reqInfo, resInfo);
		}
	}

	public RpcClient getRcpClient() {
		return rpcClient;
	}

	public RpcServiceRegistry getRpcServiceRegistry() {
		return rpcServiceRegistry;
	}

}
