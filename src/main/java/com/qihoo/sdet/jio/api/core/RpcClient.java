package com.qihoo.sdet.jio.api.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.qihoo.sdet.jio.api.listener.TcpConnectionEventListener;
import com.qihoo.sdet.jio.api.logging.RpcLogEntry.RpcPayloadInfo;
import com.qihoo.sdet.jio.api.logging.RpcLogger;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcCancel;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcError;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcRequest;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.RpcResponse;
import com.qihoo.sdet.jio.api.protocol.RpcProtocol.WirePayload;
import com.qihoo.sdet.jio.api.stream.IStreamReady;
import com.qihoo.sdet.jio.api.stream.StreamWatch;


public class RpcClient implements RpcClientChannel {

	private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);
	
	private AtomicInteger correlationId = new AtomicInteger(1);

	private final Map<Integer, PendingClientCallState> pendingRequestMap = new ConcurrentHashMap<Integer, PendingClientCallState>();
	
	private final IoSession channel;
	private final boolean reconnect;
	private final PeerInfo clientInfo;
	private final PeerInfo serverInfo;
	private final StreamWatch stream;
	private final Bootstrap bootstrap;
	private TcpConnectionEventListener eventListener;
	
	private RpcLogger rpcLogger;
	
	public RpcClient(Bootstrap bootstrap, IoSession channel, PeerInfo clientInfo, PeerInfo serverInfo, StreamWatch stream, boolean reconnect, TcpConnectionEventListener eventListener) {
		this.bootstrap = bootstrap;
		this.channel = channel;
		this.clientInfo = clientInfo;
		this.serverInfo = serverInfo;
		this.stream = stream;
		this.reconnect = reconnect;
		this.eventListener = eventListener;
	}
	
	public PeerInfo getPeerInfo() {
		return serverInfo;
	}
	
	public PeerInfo getLocalInfo(){
		return clientInfo;
	}
	
	public Bootstrap getBootstrap(){
		return bootstrap;
	}
	
	public IoSession getSession(){
		return channel;
	}
	
	public TcpConnectionEventListener listener(){
		return eventListener;
	}
	
	public void callMethod(MethodDescriptor method, RpcController controller,
			Message request, Message responsePrototype,
			RpcCallback<Message> done) {
		ClientRpcController rpcController = (ClientRpcController)controller;
		
		int correlationId = getNextCorrelationId();
		rpcController.setCorrelationId(correlationId);

		PendingClientCallState state = new PendingClientCallState(rpcController, method, responsePrototype, request, done);
		registerPendingRequest(correlationId, state);
		
		RpcRequest rpcRequest = RpcRequest.newBuilder()
			.setCorrelationId(correlationId)
			.setServiceIdentifier(state.getServiceIdentifier())
			.setMethodIdentifier(state.getMethodIdentifier())
			.setRequestBytes(request.toByteString())
			.build();
		
		if ( channel.isConnected() ){
			if ( LOGGER.isDebugEnabled() )
//				LOGGER.debug("Sending ["+rpcRequest.getCorrelationId()+"]RpcRequest.");
				LOGGER.debug("Sending ["+rpcRequest.getCorrelationId()+"]RpcRequest. signature {}", state.getMethodDesc().getFullName());
			WirePayload payload = WirePayload.newBuilder().setRpcRequest(rpcRequest).build();
			channel.write(payload);
		}else{
			LOGGER.error("Sending ["+rpcRequest.getCorrelationId()+"]RpcRequest. FAILURE");
			String errorMessage = "Session [" + channel + "]closed";
			RpcError rpcError = RpcError.newBuilder()
					.setCorrelationId(correlationId)
					.setErrorMessage(errorMessage).build();
			error(rpcError);
			doLog(state, rpcError, errorMessage);
		}
	}

	public Message callBlockingMethod(MethodDescriptor method,
			RpcController controller, Message request, Message responsePrototype)
			throws ServiceException {
		BlockingRpcCallback callback = new BlockingRpcCallback();
		
		callMethod( method, controller, request, responsePrototype, callback );
		if ( !callback.isDone() ) {
			synchronized(callback) {
				while(!callback.isDone()) {
					try {
						callback.wait();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}
		if (controller.failed()) {
			throw new ServiceException(controller.errorText());
		}
		return callback.getMessage();
	}
	
	public RpcController newRpcController() {
		return new ClientRpcController(this);
	}
	
	public String toString() {
		return "RpcClientChannel->" + getPeerInfo();
	}
	
	public void error(RpcError rpcError) {
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("Received ["+rpcError.getCorrelationId()+"]RpcError. ErrorMessage=" + rpcError.getErrorMessage());
		PendingClientCallState state = removePendingRequest(rpcError.getCorrelationId());
		if ( state != null ) {
			
			doLog( state, rpcError, rpcError.getErrorMessage() );
			
			state.handleFailure(rpcError.getErrorMessage());
		} else {
			// this can happen when we have cancel and the server still responds.
			if ( LOGGER.isDebugEnabled() )
				LOGGER.debug("No PendingCallState found for correlationId " + rpcError.getCorrelationId());
		}
	}
	
	public void response(RpcResponse rpcResponse) {
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("Received ["+rpcResponse.getCorrelationId()+"]RpcResponse.");
		PendingClientCallState state = removePendingRequest(rpcResponse.getCorrelationId());
		if ( state != null ) {
			Message response = null;
			try {
				response = state.getResponsePrototype().newBuilderForType().mergeFrom(rpcResponse.getResponseBytes()).build();
				doLog( state, response, null );
				state.handleResponse( response );

			} catch ( Exception e){
				LOGGER.error("Handler [{}]RpcResponse exception. {}", rpcResponse.getCorrelationId(), e);
				String errorMessage = "Exception: "  + e.getMessage();
				
				doLog( state, rpcResponse, errorMessage );
				
				state.handleFailure(errorMessage);
			}
		} else {
			// this can happen when we have cancel and the server still responds.
			if ( LOGGER.isDebugEnabled() )
				LOGGER.debug("No PendingClientCallState found for correlationId " + rpcResponse.getCorrelationId());
		}
	}
	
	
	void startCancel(int correlationId ) {
		PendingClientCallState state = removePendingRequest(correlationId);
		if ( state != null ) {
			RpcCancel rpcCancel = RpcCancel.newBuilder().setCorrelationId(correlationId).build();
			if ( LOGGER.isDebugEnabled() )
				LOGGER.debug("Sending ["+rpcCancel.getCorrelationId()+"]RpcCancel.");
			WirePayload payload = WirePayload.newBuilder().setRpcCancel(rpcCancel).build();
			channel.write( payload ).awaitUninterruptibly();
			
			String errorMessage = "Cancel";
			
			doLog(state, rpcCancel, errorMessage);

			state.handleFailure(errorMessage);
			
		} else {
			// this can happen if the server call completed in the meantime.
			if ( LOGGER.isDebugEnabled() )
				LOGGER.debug("No PendingClientCallState found for correlationId " + correlationId);
		}
	}

	public void close() {
		channel.close(true).awaitUninterruptibly();
	}
	
	public void handleClosure() {
		List<Integer> pendingCallIds = new ArrayList<Integer>();
		pendingCallIds.addAll(pendingRequestMap.keySet());
		do {
			for( Integer correlationId : pendingCallIds ) {
				PendingClientCallState state = removePendingRequest(correlationId);
				if ( state != null ) {
					RpcError rpcError = RpcError.newBuilder().setCorrelationId(correlationId).setErrorMessage("Forced Closure").build();
					
					doLog( state, rpcError, rpcError.getErrorMessage() );
					
					state.handleFailure(rpcError.getErrorMessage());
				}
			}
		} while( pendingRequestMap.size() > 0 );
	}
	
	protected void doLog( PendingClientCallState state, Message response, String errorMessage ) {
		if ( rpcLogger != null ) {
			RpcPayloadInfo reqInfo = RpcPayloadInfo.newBuilder().setSize(state.getRequest().getSerializedSize()).setTs(state.getStartTimestamp()).build();
			RpcPayloadInfo resInfo = RpcPayloadInfo.newBuilder().setSize(response.getSerializedSize()).setTs(System.currentTimeMillis()).build();
			rpcLogger.logCall(clientInfo, serverInfo, state.getMethodDesc().getFullName(), state.getRequest(), response, errorMessage, state.getController().getCorrelationId(), reqInfo, resInfo);
		}
	}
	
	private int getNextCorrelationId() {
		return correlationId.getAndIncrement();
	}
	
	private void registerPendingRequest(int seqId, PendingClientCallState state) {
		if (pendingRequestMap.containsKey(seqId)) {
			throw new IllegalArgumentException("State already registered");
		}
		pendingRequestMap.put(seqId, state);
	}

	private PendingClientCallState removePendingRequest(int seqId) {
		return pendingRequestMap.remove(seqId);
	}

	public static class ClientRpcController implements RpcController {

		private RpcClient rpcClient;
		private int correlationId;
		
		private String reason;
		private boolean failed;
		
		public ClientRpcController( RpcClient rpcClient ) {
			this.rpcClient = rpcClient;
		}
		
		public String errorText() {
			return reason;
		}

		public boolean failed() {
			return failed;
		}

		public boolean isCanceled() {
			throw new IllegalStateException("Serverside use only.");
		}

		public void notifyOnCancel(RpcCallback<Object> callback) {
			throw new IllegalStateException("Serverside use only.");
		}

		public void reset() {
			reason = null;
			failed = false;
			correlationId = 0;
		}

		public void setFailed(String reason) {
			this.reason = reason;
			this.failed = true;
		}

		public void startCancel() {
			rpcClient.startCancel(correlationId);
		}

		public int getCorrelationId() {
			return this.correlationId;
		}
		
		public void setCorrelationId( int correlationId ) {
			this.correlationId = correlationId;
		}

	}

	private static class BlockingRpcCallback implements RpcCallback<Message> {

		private boolean done = false;
		private Message message;
		
		public void run(Message message) {
			this.message = message;
			synchronized(this) {
				done = true;
				notify();
			}
		}
		
		public Message getMessage() {
			return message;
		}
		
		public boolean isDone() {
			return done;
		}
	}
		

	private static class PendingClientCallState {
		
		private final ClientRpcController controller;
		private final RpcCallback<Message> callback;
		private final MethodDescriptor methodDesc;
		private final Message responsePrototype;
		private final long startTimestamp;
		private final Message request;
		
		public PendingClientCallState(ClientRpcController controller, MethodDescriptor methodDesc, Message responsePrototype, Message request, RpcCallback<Message> callback) {
			this.controller = controller;
			this.methodDesc = methodDesc;
			this.responsePrototype = responsePrototype;
			this.callback = callback;
			this.startTimestamp = System.currentTimeMillis();
			this.request = request;
		}

		public String getServiceIdentifier() {
			return methodDesc.getService().getName();
		}
		
		public String getMethodIdentifier() {
			return methodDesc.getName();
		}
		
		public void handleResponse( Message response ) {
			callback(response);
		}
		
		public void handleFailure( String message ) {
			controller.setFailed(message);
			callback(null);
		}

		private void callback(Message message) {
			if ( callback != null ) {
				callback.run(message);
			}
		}

		public long getStartTimestamp() {
			return startTimestamp;
		}

		public Message getRequest() {
			return request;
		}

		public ClientRpcController getController() {
			return controller;
		}

		public MethodDescriptor getMethodDesc() {
			return methodDesc;
		}

		public Message getResponsePrototype() {
			return responsePrototype;
		}
	}
	
	
	public String synStream(String path, RpcClientChannel rpcChannel) throws Exception {
		return stream.synSendStream(path, rpcChannel);
	};
	
	
	public PeerInfo getClientInfo() {
		return clientInfo;
	}

	public PeerInfo getServerInfo() {
		return serverInfo;
	}

	public IoSession getChannel() {
		return channel;
	}

	public RpcLogger getCallLogger() {
		return rpcLogger;
	}

	public void setCallLogger(RpcLogger callLogger) {
		this.rpcLogger = callLogger;
	}

	@Override
	public void stream(String path, IStreamReady ready) {
		stream.sendStream(path, this, ready);
	}

	@Override
	public String synStream(String path) throws Exception {
		if ( stream == null ){
			return null;
		}
		return stream.synSendStream(path, this);
	}

	@Override
	public boolean reconnect() {
		return reconnect;
	}
}
