package com.qihoo.sdet.jio.api.core;

import java.net.InetSocketAddress;

public class PeerInfo {

	private String hostName;
	private int port;
	private String pid;
	

	public PeerInfo( InetSocketAddress localAddress ) {
		this(localAddress.getHostName(), localAddress.getPort());
	}
	
	public PeerInfo( String hostName, int port ) {
		this.hostName = hostName;
		this.port = port;
		String pid = System.getProperty("pid","<NONE>");
		this.pid = pid;
	}

	public PeerInfo( String hostName, int port, String pid ) {
		this.hostName = hostName;
		this.port = port;
		this.pid = pid;
	}
	
	public void setPid(String pid){
		this.pid = pid;
	}
	
	public String toString() {
		return getName() + "[" + getPid() + "]";
	}
	
	public String getName() {
		return getHostName() + ":" + getPort();
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getPid() {
		return pid;
	}
		
}
