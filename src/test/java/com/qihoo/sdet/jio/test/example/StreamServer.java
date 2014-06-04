package com.qihoo.sdet.jio.test.example;

import java.io.File;
import java.io.FilenameFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qihoo.sdet.jio.api.core.RpcClientChannel;
import com.qihoo.sdet.jio.api.server.TcpServer;
import com.qihoo.sdet.jio.api.stream.IStreamReady;

@SuppressWarnings("rawtypes")
public class StreamServer extends TcpServer {
	
	static int fail_num = 0;
	static int success_num = 0;
	static int total_num = 0;
	static int max_timeout = 30;
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamServer.class);
	
	public void sessionClosed(RpcClientChannel rpcChannel){
		LOGGER.debug("sessionClosed ...{}", rpcChannel.getSession());
	}
	
	public void sessionOpen(RpcClientChannel rpcChannel){
		LOGGER.debug("sessionOpen ...{}", rpcChannel.getSession().getRemoteAddress());
//		sendFile(rpcChannel);
	}
	
	static public void waitSendFinish() throws InterruptedException{
		while(fail_num + success_num < total_num  && max_timeout > 0  && total_num > 0){
			Thread.sleep(1000);
			max_timeout--;
		}
	}
	
	public static boolean sendOK(){
		return ( fail_num + success_num  == total_num ) && (fail_num == 0);
	}
	
	public static void sendFile(RpcClientChannel rpcChannel){
		File []  fs = collectApk("src/test/resources");
		for (final File f: fs){
			try {
				LOGGER.info("sending......{}", f.getAbsolutePath());
				rpcChannel.stream(f.getAbsolutePath(), new IStreamReady(){
					@Override
					public void ready(String path, String peer)
					{
						LOGGER.info("file trans success. souce: {}  peer: {}", path, peer);
						success_num ++;
					}

					@Override
					public void exceptCaught(Throwable e)
					{
						LOGGER.info("file trans failure. souce: {}", f.getAbsolutePath());
						fail_num ++;
					}
				});
				total_num ++;
			} catch ( Exception e ) {
				e.printStackTrace();
				LOGGER.error("sending failed: " + e.getMessage());
			}
		}
	}
	
	static File[] collectApk(String resPath)
	{
		File resDir = new File(resPath);
		
		File[] apks = resDir.listFiles(
				new FilenameFilter()
				{
					@Override
					public boolean accept(File dir, String name)
					{
						if (name.endsWith(".apk"))
						{
							return true;
						}
						return false;
					}
				});
		return apks;
	}
}
