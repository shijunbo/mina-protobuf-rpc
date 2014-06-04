package com.qihoo.sdet.jio.api.heartbeat;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.keepalive.KeepAliveFilter;
import org.apache.mina.filter.keepalive.KeepAliveRequestTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatFilter extends KeepAliveFilter{
	
	private static final int INTERVAL = 120;//in seconds
    private static final int TIMEOUT = 60; //in seconds
      
    public HeartBeatFilter(HeartBeatMessageFactory messageFactory) {  
        super(messageFactory, IdleStatus.BOTH_IDLE, new HeartBeatHandler(), INTERVAL, TIMEOUT);
    }  
      
    public HeartBeatFilter() {  
        super(new HeartBeatMessageFactory(), IdleStatus.BOTH_IDLE, new HeartBeatHandler(), INTERVAL, TIMEOUT);  
        this.setForwardEvent(false);
    }
    
    static class HeartBeatHandler implements KeepAliveRequestTimeoutHandler {
    	
    	private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatHandler.class);
    	
    	public void keepAliveRequestTimedOut(KeepAliveFilter filter, IoSession session) throws Exception {
    		LOGGER.warn("Connection lost, session will be closed {}", session);
            session.close(true);   
        }
    }
}
